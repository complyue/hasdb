
module DB.RT where

import           Prelude
-- import           Debug.Trace

-- import           System.IO.Unsafe

import           Control.Monad.Reader
-- import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.HashMap.Strict           as Map
import qualified Data.Text                     as T
import           Data.Dynamic

import           Data.Lossless.Decimal          ( castDecimalToInteger )
import           Language.Edh.EHI

import           DB.Storage.DataDir
import           DB.Storage.InMem


-- | utility className(*args,**kwargs)
classNameProc :: EdhProcedure
classNameProc !argsSender !exit = do
  !pgs <- ask
  let callerCtx   = edh'context pgs
      callerScope = contextScope callerCtx
  packEdhArgs argsSender $ \(ArgsPack !args !kwargs) ->
    let !argsCls = classNameOf <$> args
    in  if Map.null kwargs
          then case argsCls of
            [] ->
              exitEdhProc exit (EdhClass $ objClass $ thisObject callerScope)
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
newBoProc :: EdhProcedure
newBoProc !argsSender !exit =
  packEdhArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhClass !cls, EdhObject !sbObj] | Map.null kwargs ->
      createEdhObject cls [] $ \(OriginalValue boVal _ _) -> case boVal of
        EdhObject !bo -> do
          pgs <- ask
          let world = contextWorld $ edh'context pgs
          contEdhSTM $ do
            boScope <- mkScopeWrapper world $ objectScope bo
            modifyTVar' (objSupers bo) (sbObj :)
            changeEntityAttr (objEntity sbObj) (AttrByName "_boScope")
              $ EdhObject boScope
            exitEdhSTM pgs exit $ EdhObject bo
        _ -> error "bug: createEdhObject returned non-object"
    _ -> throwEdh EvalError "Invalid arg to `newBo`"


-- | utility streamToDisk(persistOutlet, dataFileFolder, sinkBaseDFD)
--
-- this should be called from the main Edh thread, block it here until
-- db shutdown, or other Edh threads will be terminated, including
-- the one running the db app.
streamToDiskProc :: EdhProcedure
streamToDiskProc !argsSender !exit =
  packEdhArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhSink !persistOutlet, EdhString !dataFileFolder, EdhSink !sinkBaseDFD]
      | Map.null kwargs -> edhWaitIO exit $ do
        -- not to use `unsafeIOToSTM` here, despite it being retry prone,
        -- nested `atomically` is prohibited as well.
        streamEdhReprToDisk persistOutlet (T.unpack dataFileFolder) sinkBaseDFD
        return nil
    _ -> throwEdh EvalError "Invalid arg to `streamToDisk`"

-- | utility streamFromDisk(restoreOutlet, baseDFD)
streamFromDiskProc :: EdhProcedure
streamFromDiskProc !argsSender !exit = do
  pgs <- ask
  packEdhArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhSink !restoreOutlet, EdhDecimal baseDFD] | Map.null kwargs ->
      -- not to use `unsafeIOToSTM` here, despite it being retry prone,
      -- nested `atomically` is prohibited as well.
      edhWaitIO exit $ do
        streamEdhReprFromDisk (edh'context pgs) restoreOutlet
          $ fromIntegral
          $ castDecimalToInteger baseDFD
        return nil
    _ -> throwEdh EvalError "Invalid arg to `streamFromDisk`"


boiReindexProc :: EdhProcedure
boiReindexProc !argsSndr !exit = packEdhArgs argsSndr $ \case
  (ArgsPack [EdhObject !bo] !kwargs) | Map.null kwargs -> do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ fromDynamic <$> readTVar es >>= \case
      Nothing               -> error "bug: this is not a boi"
      Just (boi :: BoIndex) -> do
        boi' <- case boi of
          UniqueIndex idx -> -- unique index
            UniqueIndex <$> reindexUniqBusObj pgs bo idx
          NonUniqueIndex idx -> -- non-unique index
            NonUniqueIndex <$> reindexNouBusObj pgs bo idx
        writeTVar es $ toDyn boi'
        exitEdhSTM pgs exit $ EdhObject this
  _ -> throwEdh EvalError "Invalid args to boiReindexProc"


-- | host constructor BoIndex( indexSpec, unique=false )
boiHostCtor
  :: Scope       -- constructor scope
  -> ArgsSender  -- construction args
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store 
  -> STM ()
boiHostCtor !scope _ obs = do
  methods <- sequence
    [ (AttrByName nm, ) <$> mkHostProc scope EdhMethod nm hp args
    | (nm, hp, args) <-
      [ ( "__init__"
        , __init__
        , PackReceiver
          [ RecvArg "indexSpec" Nothing Nothing
          , RecvArg "unique"    Nothing (Just (LitExpr (BoolLiteral False)))
          ]
        )
      , ("<-", boiReindexProc, PackReceiver [RecvArg "bo" Nothing Nothing])
      , ("[]", boiLookupProc , PackReceiver [RecvRestPosArgs "keyValues"])
      , ( "groups"
        , boiRangeProc
        , PackReceiver
          [ RecvArg "start" Nothing (Just (GodSendExpr edhNone))
          , RecvArg "until" Nothing (Just (GodSendExpr edhNone))
          ]
        )
      , ( "range"
        , boiRangeProc
        , PackReceiver
          [ RecvArg "start" Nothing (Just (GodSendExpr edhNone))
          , RecvArg "until" Nothing (Just (GodSendExpr edhNone))
          ]
        )
      ]
    ]
  writeTVar obs $ Map.fromList methods

 where

  __init__ :: EdhProcedure
  __init__ !argsSndr !exit = packEdhArgs argsSndr $ \case
    (ArgsPack [EdhExpr _ !idxSpecExpr _] !kwargs) -> do
      pgs <- ask
      contEdhSTM $ do
        uniqIdx <- case Map.lookup "unique" kwargs of
          Nothing          -> return False
          Just (EdhBool b) -> return b
          Just v ->
            throwEdhSTM pgs EvalError
              $  "Invalid unique arg value type: "
              <> T.pack (show $ edhTypeOf v)
        exitEdhSTM pgs exit nil
    _ -> throwEdh EvalError $ "Invalid argument to BoIndex(): " <> T.pack
      (show argsSndr)

  boiLookupProc :: EdhProcedure
  boiLookupProc = undefined

  -- | host generator idx.range( since, until )
  boiRangeProc :: EdhProcedure
  boiRangeProc !argsSender _ = ask >>= \pgs ->
    case generatorCaller $ edh'context pgs of
      Nothing          -> throwEdh EvalError "Can only be called as generator"
      Just genr'caller -> case argsSender of
        [SendPosArg !nExpr] -> evalExpr nExpr $ \(OriginalValue nVal _ _) ->
          case nVal of
            (EdhDecimal (Decimal d e n)) | d == 1 -> contEdhSTM
              $ yieldOneEntity (fromIntegral n * 10 ^ (e + 6)) genr'caller
            _ ->
              throwEdh EvalError $ "Invalid argument: " <> T.pack (show nVal)
        _ ->
          throwEdh EvalError $ "Invalid argument: " <> T.pack (show argsSender)

