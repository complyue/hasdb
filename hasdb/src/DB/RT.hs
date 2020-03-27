
module DB.RT where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader
-- import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.HashMap.Strict           as Map
import           Data.Map.Strict               as TreeMap
import qualified Data.HashSet                  as Set
import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import           Data.Dynamic

import           Data.Lossless.Decimal          ( castDecimalToInteger )
import           Language.Edh.EHI

import           DB.Storage.DataDir
import           DB.Storage.InMem


-- | utility className(*args,**kwargs)
classNameProc :: EdhProcedure
classNameProc (ArgsPack !args !kwargs) !exit = do
  !pgs <- ask
  let callerCtx   = edh'context pgs
      callerScope = contextScope callerCtx
      !argsCls    = classNameOf <$> args
  if Map.null kwargs
    then case argsCls of
      []  -> exitEdhProc exit (EdhClass $ objClass $ thisObject callerScope)
      [t] -> exitEdhProc exit t
      _   -> exitEdhProc exit (EdhTuple argsCls)
    else exitEdhProc
      exit
      (EdhArgsPack $ ArgsPack argsCls $ Map.map classNameOf kwargs)
 where
  classNameOf :: EdhValue -> EdhValue
  classNameOf (EdhClass (ProcDefi _ _ (ProcDecl !cn _ _))) = EdhString cn
  classNameOf (EdhObject !obj) =
    EdhString $ procedure'name $ procedure'decl $ objClass obj
  classNameOf _ = nil


-- | utility newBo(boClass, sbObj)
-- this performs non-standard business object construction
newBoProc :: EdhProcedure
newBoProc (ArgsPack !args !kwargs) !exit = case args of
  [EdhClass !cls, EdhObject !sbObj] | Map.null kwargs ->
    createEdhObject cls (ArgsPack [] Map.empty) $ \(OriginalValue boVal _ _) ->
      case boVal of
        EdhObject !bo -> do
          pgs <- ask
          let ctx = edh'context pgs
          contEdhSTM $ do
            boScope <- mkScopeWrapper ctx $ objectScope ctx bo
            modifyTVar' (objSupers bo) (sbObj :)
            changeEntityAttr pgs (objEntity sbObj) (AttrByName "_boScope")
              $ EdhObject boScope
            lookupEntityAttr pgs (objEntity bo) (AttrByName "__db_init__")
              >>= \case
                    EdhNil -> exitEdhSTM pgs exit $ EdhObject bo
                    EdhMethod !mth'proc ->
                      runEdhProc pgs
                        $ callEdhMethod bo mth'proc (ArgsPack [] Map.empty)
                        $ \_ -> contEdhSTM $ exitEdhSTM pgs exit $ EdhObject bo
                    !badMth ->
                      throwEdhSTM pgs EvalError
                        $  "Invalid __db_init__() method type: "
                        <> T.pack (show $ edhTypeOf badMth)
        _ -> error "bug: createEdhObject returned non-object"
  _ -> throwEdh EvalError "Invalid arg to `newBo`"


-- | utility streamToDisk(persistOutlet, dataFileFolder, sinkBaseDFD)
--
-- this should be called from the main Edh thread, block it here until
-- db shutdown, or other Edh threads will be terminated, including
-- the one running the db app.
streamToDiskProc :: EdhProcedure
streamToDiskProc (ArgsPack !args !kwargs) !exit = do
  pgs <- ask
  case args of
    [EdhSink !persistOutlet, EdhString !dataFileFolder, EdhSink !sinkBaseDFD]
      | Map.null kwargs -> edhWaitIO exit $ do
        -- not to use `unsafeIOToSTM` here, despite it being retry prone,
        -- nested `atomically` is prohibited as well.
        streamEdhReprToDisk (edh'context pgs)
                            persistOutlet
                            (T.unpack dataFileFolder)
                            sinkBaseDFD
        return nil
    _ -> throwEdh EvalError "Invalid arg to `streamToDisk`"

-- | utility streamFromDisk(restoreOutlet, baseDFD)
streamFromDiskProc :: EdhProcedure
streamFromDiskProc (ArgsPack !args !kwargs) !exit = do
  pgs <- ask
  case args of
    [EdhSink !restoreOutlet, EdhDecimal baseDFD] | Map.null kwargs ->
      -- not to use `unsafeIOToSTM` here, despite it being retry prone,
      -- nested `atomically` is prohibited as well.
      edhWaitIO exit $ do
        streamEdhReprFromDisk (edh'context pgs) restoreOutlet
          $ fromIntegral
          $ castDecimalToInteger baseDFD
        return nil
    _ -> throwEdh EvalError "Invalid arg to `streamFromDisk`"


-- | host constructor BoIndex( indexSpec, unique=false )
boiHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store 
  -> STM ()
boiHostCtor !pgsCtor _ !obs = do
  let !scope = contextScope $ edh'context pgsCtor
  methods <- sequence
    [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp args
    | (nm, vc, hp, args) <-
      [ ( "__init__"
        , EdhMethod
        , __init__
        , PackReceiver
          [ RecvRestPosArgs "indexSpec"
          , RecvArg "unique" Nothing (Just (LitExpr (BoolLiteral False)))
          ]
        )
      , ( "<-"
        , EdhMethod
        , boiReindexProc
        , PackReceiver [RecvArg "bo" Nothing Nothing]
        )
      , ( "^*"
        , EdhMethod
        , boiThrowAwayProc
        , PackReceiver [RecvArg "bo" Nothing Nothing]
        )
      , ( "[]"
        , EdhMethod
        , boiLookupProc
        , PackReceiver [RecvArg "keyValues" Nothing Nothing]
        )
      , ( "groups"
        , EdhGnrtor
        , boiGroupsOrRangeProc listBoIndexGroups
        , PackReceiver
          [ RecvArg "min" Nothing (Just (GodSendExpr edhNone))
          , RecvArg "max" Nothing (Just (GodSendExpr edhNone))
          ]
        )
      , ( "range"
        , EdhGnrtor
        , boiGroupsOrRangeProc listBoIndexRange
        , PackReceiver
          [ RecvArg "min" Nothing (Just (GodSendExpr edhNone))
          , RecvArg "max" Nothing (Just (GodSendExpr edhNone))
          ]
        )
      ]
    ]
  writeTVar obs $ Map.fromList methods

 where

  __init__ :: EdhProcedure
  __init__ (ArgsPack !args !kwargs) !exit = do
    pgs <- ask
    let
      this = thisObject $ contextScope $ edh'context pgs
      doIt :: Bool -> STM ()
      doIt !uniqIdx = do
        parseIndexSpec pgs args $ \spec@(IndexSpec spec') -> do
          let specStr = T.pack $ show spec
              idxName = case Map.lookup "name" kwargs of
                Nothing                 -> "<index>"
                Just (EdhString keyStr) -> keyStr
                Just v                  -> T.pack $ show v
          modifyTVar' obs
            $ Map.insert (AttrByName "unique") (EdhBool uniqIdx)
            . Map.insert (AttrByName "spec") (EdhString specStr)
            . Map.insert (AttrByName "keys")
                         (EdhTuple $ attrKeyValue . fst <$> spec')
            . Map.insert (AttrByName "name") (EdhString idxName)
            . Map.insert
                (AttrByName "__repr__")
                (  EdhString
                $  (if uniqIdx then "Unique " else "Index ")
                <> idxName
                <> " "
                <> specStr
                )
          let boi = if uniqIdx
                then UniqueIndex $ UniqBoIdx spec TreeMap.empty Map.empty
                else NonUniqueIndex $ NouBoIdx spec TreeMap.empty Map.empty
          boiVar <- newTMVar boi
          writeTVar (entity'store $ objEntity this) $ toDyn boiVar
          exitEdhSTM pgs exit $ EdhObject this
    contEdhSTM $ case Map.lookup "unique" kwargs of
      Nothing          -> doIt False
      Just (EdhBool b) -> doIt b
      Just v ->
        throwEdhSTM pgs EvalError $ "Invalid unique arg value type: " <> T.pack
          (show $ edhTypeOf v)

  boiName :: STM Text
  boiName = Map.lookup (AttrByName "name") <$> readTVar obs >>= \case
    Nothing               -> return "<index>"
    Just (EdhString name) -> return name
    Just v                -> return $ T.pack $ show v

  boiReindexProc :: EdhProcedure
  boiReindexProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs EvalError $ "bug: this is not a boi : " <> T.pack
              (show esd)
          Just (boiVar :: TMVar BoIndex) -> do
    -- index update is expensive, use TMVar to avoid computation being retried
            boi     <- takeTMVar boiVar
            idxName <- boiName
            reindexBusinessObject idxName boi pgs bo $ \boi' -> do
              putTMVar boiVar boi'
              exitEdhSTM pgs exit nil
    _ -> throwEdh EvalError "Invalid args to boiReindexProc"

  boiThrowAwayProc :: EdhProcedure
  boiThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs EvalError $ "bug: this is not a boi : " <> T.pack
              (show esd)
          Just (boiVar :: TMVar BoIndex) -> do
    -- index update is expensive, use TMVar to avoid computation being retried
            boi <- takeTMVar boiVar
            throwAwayIndexedObject boi pgs bo $ \boi' -> do
              putTMVar boiVar boi'
              exitEdhSTM pgs exit nil
    _ -> throwEdh EvalError "Invalid args to boiThrowAwayProc"

  boiLookupProc :: EdhProcedure
  boiLookupProc (ArgsPack !args !kwargs) !exit = case args of
    [keyValues] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs EvalError $ "bug: this is not a boi : " <> T.pack
              (show esd)
          Just (boiVar :: TMVar BoIndex) -> do
            boi <- readTMVar boiVar
            edhIdxKeyVals pgs keyValues $ \case
              Nothing          -> exitEdhSTM pgs exit nil
              Just !idxKeyVals -> do
                result <- lookupBoIndex boi idxKeyVals
                exitEdhSTM pgs exit result
    _ -> throwEdh EvalError "Invalid args to boiLookupProc"

  -- | host generator idx.groups/range( min=None, max=None )
  boiGroupsOrRangeProc
    :: (  BoIndex
       -> Maybe [Maybe IdxKeyVal]
       -> Maybe [Maybe IdxKeyVal]
       -> STM [(IndexKey, EdhValue)]
       )
    -> EdhProcedure
  boiGroupsOrRangeProc fn !apk !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdh EvalError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> do
        let
          yieldResult :: [(IndexKey, EdhValue)] -> STM ()
          yieldResult [] = return ()
          yieldResult ((ik, v) : rest) =
            runEdhProc pgs'
              $ iter'cb
                  (EdhArgsPack
                    (ArgsPack [edhValueOfIndexKey ik, noneNil v] Map.empty)
                  )
              $ \_ -> yieldResult rest
        case parseIdxRng apk of
          Left argsErr ->
            throwEdh EvalError $ argsErr <> " for boiGroupsOrRangeProc"
          Right (minKey, maxKey) -> contEdhSTM $ do
            esd <- readTVar es
            case fromDynamic esd of
              Nothing ->
                throwEdhSTM pgs EvalError
                  $  "bug: this is not a boi : "
                  <> T.pack (show esd)
              Just (boiVar :: TMVar BoIndex) -> do
                boi <- readTMVar boiVar
                edhIdxKeyVals pgs minKey $ \minKeyVals ->
                  edhIdxKeyVals pgs maxKey $ \maxKeyVals -> do
                    result <- fn boi minKeyVals maxKeyVals
                    yieldResult result
                    exitEdhSTM pgs exit nil


type IdxRng = (EdhValue, EdhValue)
parseIdxRng :: ArgsPack -> Either Text IdxRng
parseIdxRng =
  parseArgsPack (nil, nil)
    $ ArgsPackParser
        [ \v (_, argMax) -> Right (v, argMax)
        , \v (argMin, _) -> Right (argMin, v)
        ]
    $ Map.fromList
        [ ("min", \v (_, argMax) -> Right (v, argMax))
        , ("max", \v (argMin, _) -> Right (argMin, v))
        ]


-- | host constructor BoSet()
bosHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store 
  -> STM ()
bosHostCtor !pgsCtor _ !obs = do
  let !scope = contextScope $ edh'context pgsCtor
      !this  = thisObject scope
  bosVar <- newTMVar (Set.empty :: BoSet)
  writeTVar (entity'store $ objEntity this) $ toDyn bosVar
  methods <- sequence
    [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp args
    | (nm, vc, hp, args) <-
      [ ( "<-"
        , EdhMethod
        , bosAddProc
        , PackReceiver [RecvArg "bo" Nothing Nothing]
        )
      , ( "^*"
        , EdhMethod
        , bosThrowAwayProc
        , PackReceiver [RecvArg "bo" Nothing Nothing]
        )
      , ("all", EdhGnrtor, bosAllProc, PackReceiver [])
      ]
    ]
  writeTVar obs $ Map.fromList methods

 where

  bosAddProc :: EdhProcedure
  bosAddProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs EvalError $ "bug: this is not a bos : " <> T.pack
              (show esd)
          Just (bosVar :: TMVar BoSet) -> do
            bos <- takeTMVar bosVar
            putTMVar bosVar $ Set.insert bo bos
            exitEdhSTM pgs exit nil
    _ -> throwEdh EvalError "Invalid args to bosAddProc"

  bosThrowAwayProc :: EdhProcedure
  bosThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs EvalError $ "bug: this is not a bos : " <> T.pack
              (show esd)
          Just (bosVar :: TMVar BoSet) -> do
            bos <- takeTMVar bosVar
            putTMVar bosVar $ Set.delete bo bos
            exitEdhSTM pgs exit nil
    _ -> throwEdh EvalError "Invalid args to bosThrowAwayProc"

  -- | host generator bos.all()
  bosAllProc :: EdhProcedure
  bosAllProc _ !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdh EvalError "Can only be called as generator"
      Just (!pgs', !iter'cb) ->
        let yieldResult :: [Object] -> STM ()
            yieldResult [] = return ()
            yieldResult (bo : rest) =
                runEdhProc pgs' $ iter'cb (EdhObject bo) $ \_ -> yieldResult rest
        in  contEdhSTM $ do
              esd <- readTVar es
              case fromDynamic esd of
                Nothing ->
                  throwEdhSTM pgs EvalError
                    $  "bug: this is not a bos : "
                    <> T.pack (show esd)
                Just (bosVar :: TMVar BoSet) -> do
                  bos <- readTMVar bosVar
                  yieldResult $ Set.toList bos
                  exitEdhSTM pgs exit nil

