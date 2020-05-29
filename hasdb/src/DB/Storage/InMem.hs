
module DB.Storage.InMem where

import           Prelude
-- import           Debug.Trace

import           Control.Monad
import           Control.Monad.Reader
import           Control.Exception
import           Control.Concurrent.STM

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import qualified Data.Map.Strict               as TreeMap
import qualified Data.HashSet                  as Set
import           Data.Dynamic

import qualified DB.Storage.InMem.TreeIdx      as TI

import           Language.Edh.EHI


-- | Business Object Set
type BoSet = Set.HashSet Object

-- | host constructor BoSet()
bosHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store
  -> (Dynamic -> STM ())  -- in-band data to be written to entity store
  -> STM ()
bosHostCtor !pgsCtor _ !obs !ctorExit = do
  let !scope = contextScope $ edh'context pgsCtor
  bosVar <- newTMVar (Set.empty :: BoSet)
  modifyTVar' obs . Map.union =<< Map.fromList <$> sequence
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
  ctorExit $ toDyn bosVar

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
            throwEdhSTM pgs UsageError $ "bug: this is not a bos : " <> T.pack
              (show esd)
          Just (bosVar :: TMVar BoSet) -> do
            bos <- takeTMVar bosVar
            -- catching stm exceptions here is enough, no Edh action involved
            flip
                catchSTM
                (\(e :: SomeException) -> tryPutTMVar bosVar bos >> throwSTM e)
              $ do
                  putTMVar bosVar $ Set.insert bo bos
                  exitEdhSTM pgs exit nil
    _ -> throwEdh UsageError "Invalid args to bosAddProc"

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
            throwEdhSTM pgs UsageError $ "bug: this is not a bos : " <> T.pack
              (show esd)
          Just (bosVar :: TMVar BoSet) -> do
            bos <- takeTMVar bosVar
            -- catching stm exceptions here is enough, no Edh action involved
            flip
                catchSTM
                (\(e :: SomeException) -> tryPutTMVar bosVar bos >> throwSTM e)
              $ do
                  putTMVar bosVar $ Set.delete bo bos
                  exitEdhSTM pgs exit nil
    _ -> throwEdh UsageError "Invalid args to bosThrowAwayProc"

  -- | host generator bos.all()
  bosAllProc :: EdhProcedure
  bosAllProc _ !exit = do
    pgs <- ask
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdh UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> do
        let
          yieldResults :: [Object] -> STM ()
          yieldResults [] = exitEdhSTM pgs exit nil
          yieldResults (bo : rest) =
            runEdhProc pgs' $ iter'cb (EdhObject bo) $ \case
              Left (pgsThrower, exv) ->
                edhThrowSTM pgsThrower { edh'context = edh'context pgs } exv
              Right EdhBreak         -> exitEdhSTM pgs exit nil
              Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
              _                      -> yieldResults rest
        contEdhSTM $ do
          let this = thisObject $ contextScope $ edh'context pgs
              !es  = entity'store $ objEntity this
          esd <- readTVar es
          case fromDynamic esd of
            Nothing ->
              throwEdhSTM pgs UsageError $ "bug: this is not a bos : " <> T.pack
                (show esd)
            Just (bosVar :: TMVar BoSet) -> do
              bos <- readTMVar bosVar
              yieldResults $ Set.toList bos


type AscSort = Bool
newtype IndexSpec = IndexSpec [(AttrKey, AscSort)]
  deriving (Eq)
instance Show IndexSpec where
  show (IndexSpec s) = "👉 " <> showspec s <> "👈"
   where
    ordind asc = if asc then " 🔼 " else " 🔽 "
    showspec :: [(AttrKey, AscSort)] -> String
    showspec []            = "empty"
    showspec [(attr, asc)] = show attr <> ordind asc
    showspec ((attr, asc) : rest@(_ : _)) =
      show attr <> ordind asc <> showspec rest

indexFieldSortOrderSpec :: EdhProgState -> Text -> (Bool -> STM ()) -> STM ()
indexFieldSortOrderSpec pgs !ordSpec !exit = case ordSpec of
  "ASC"        -> exit True
  "DESC"       -> exit False
  "ASCENDING"  -> exit True
  "DESCENDING" -> exit False
  "asc"        -> exit True
  "desc"       -> exit False
  "ascending"  -> exit True
  "descending" -> exit False
  _ ->
    throwEdhSTM pgs UsageError
      $  "Invalid index field sorting order: "
      <> ordSpec


parseIndexSpec :: EdhProgState -> [EdhValue] -> (IndexSpec -> STM ()) -> STM ()
parseIndexSpec pgs !args !exit = case args of
  [EdhExpr _ (AttrExpr (DirectRef (NamedAttr !attrName))) _] ->
    doExit [(AttrByName attrName, True)]
  [EdhExpr _ (InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (AttrExpr (DirectRef (NamedAttr ordSpec)))) _]
    -> indexFieldSortOrderSpec pgs ordSpec
      $ \asc -> doExit [(AttrByName attrName, asc)]
  [EdhExpr _ (InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (LitExpr (BoolLiteral !asc))) _]
    -> doExit [(AttrByName attrName, asc)]
  [EdhExpr _ (TupleExpr !fieldSpecs) _] ->
    seqcontSTM (fieldFromExpr <$> fieldSpecs) doExit
  [EdhTuple !fieldSpecs  ] -> seqcontSTM (fieldFromVal <$> fieldSpecs) doExit
  [EdhList  (List _ !fsl)] -> do
    fieldSpecs <- readTVar fsl
    seqcontSTM (fieldFromVal <$> fieldSpecs) doExit
  _ -> throwEdhSTM pgs UsageError $ "Invalid index specification: " <> T.pack
    (show args)
 where
  doExit :: [(AttrKey, AscSort)] -> STM ()
  doExit !spec = do
    when (null spec)
      $ throwEdhSTM pgs UsageError "Index specification can not be empty"
    exit $ IndexSpec spec
  fieldFromExpr :: Expr -> ((AttrKey, AscSort) -> STM ()) -> STM ()
  fieldFromExpr !x !exit' = case x of
    AttrExpr (DirectRef (NamedAttr !attrName)) ->
      exit' (AttrByName attrName, True)
    InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (AttrExpr (DirectRef (NamedAttr ordSpec)))
      -> indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (LitExpr (BoolLiteral !asc))
      -> exit' (AttrByName attrName, asc)
    TupleExpr [AttrExpr (DirectRef (NamedAttr !attrName)), AttrExpr (DirectRef (NamedAttr ordSpec))]
      -> indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    TupleExpr [AttrExpr (DirectRef (NamedAttr !attrName)), LitExpr (BoolLiteral !asc)]
      -> exit' (AttrByName attrName, asc)
    _ -> throwEdhSTM pgs UsageError $ "Invalid index field spec: " <> T.pack
      (show x)
  fieldFromVal :: EdhValue -> ((AttrKey, AscSort) -> STM ()) -> STM ()
  fieldFromVal !x !exit' = case x of
    EdhPair (EdhString !attrName) (EdhBool !asc) ->
      exit' (AttrByName attrName, asc)
    EdhTuple [EdhString !attrName, EdhBool !asc] ->
      exit' (AttrByName attrName, asc)
    EdhString !attrName -> exit' (AttrByName attrName, True)
    EdhPair (EdhString !attrName) (EdhString !ordSpec) ->
      indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    EdhTuple [EdhString !attrName, EdhString !ordSpec] ->
      indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    _ -> throwEdhSTM pgs UsageError $ "Invalid index field spec: " <> T.pack
      (show x)


-- note the order of data constructors here decides how it's sorted,
-- wrt value type difference
data IdxKeyVal =
      IdxBoolVal !Bool
    | IdxNumVal !Decimal
    | IdxStrVal !Text
    | IdxAggrVal ![Maybe IdxKeyVal]
  deriving (Eq, Ord)

edhIdxKeyVal
  :: EdhProgState -> EdhValue -> ((Maybe IdxKeyVal) -> STM ()) -> STM ()
edhIdxKeyVal _ EdhNil         !exit = exit Nothing
edhIdxKeyVal _ (EdhBool    v) !exit = exit $ Just $ IdxBoolVal v
edhIdxKeyVal _ (EdhDecimal v) !exit = exit $ Just $ IdxNumVal v
edhIdxKeyVal _ (EdhString  v) !exit = exit $ Just $ IdxStrVal v
edhIdxKeyVal !pgs (EdhTuple vs) !exit =
  seqcontSTM (edhIdxKeyVal pgs <$> vs) $ exit . Just . IdxAggrVal
-- list is mutable, meaning can change without going through entity attr
-- update, don't support it unless sth else is figured out
edhIdxKeyVal !pgs !val _ =
  throwEdhSTM pgs UsageError $ "Invalid value type to be indexed: " <> T.pack
    (edhTypeNameOf val)

edhIdxKeyVals
  :: EdhProgState -> EdhValue -> (Maybe [Maybe IdxKeyVal] -> STM ()) -> STM ()
edhIdxKeyVals _ EdhNil        !exit = exit Nothing
edhIdxKeyVals _ (EdhTuple []) !exit = exit Nothing
edhIdxKeyVals pgs (EdhTuple kvs) !exit =
  seqcontSTM (edhIdxKeyVal pgs <$> kvs) $ exit . Just
edhIdxKeyVals pgs kv !exit = edhIdxKeyVal pgs kv $ \ikv -> exit $ Just [ikv]

edhValOfIdxKey :: Maybe IdxKeyVal -> EdhValue
edhValOfIdxKey Nothing                  = nil
edhValOfIdxKey (Just (IdxBoolVal v   )) = EdhBool v
edhValOfIdxKey (Just (IdxNumVal  v   )) = EdhDecimal v
edhValOfIdxKey (Just (IdxStrVal  v   )) = EdhString v
edhValOfIdxKey (Just (IdxAggrVal ikvs)) = EdhTuple $ edhValOfIdxKey <$> ikvs


-- todo using lists here may be less efficient due to cache unfriendly,
--      is GHC smart enough to optimize these lists or better change to 
--      more cache friendly data structures with less levels of indirection ?
data IndexKey = IndexKey !IndexSpec ![Maybe IdxKeyVal]
  deriving (Eq)
instance Ord IndexKey where
  compare (IndexKey x'spec x'key) (IndexKey y'spec y'key) = if x'spec /= y'spec
    then error "bug: different index spec to compare"
    else cmp x'spec x'key y'key
   where
    cmp :: IndexSpec -> [Maybe IdxKeyVal] -> [Maybe IdxKeyVal] -> Ordering
    -- equal according to the spec
    cmp (IndexSpec []) _       _       = EQ
    -- sequence is equal regardless of spec
    cmp _              []      []      = EQ
    -- prefix match considered equal, TODO this creates problems ?
    cmp _              (_ : _) []      = EQ
    cmp _              []      (_ : _) = EQ
    -- both nil considered equal
    cmp (IndexSpec (_ : s'rest)) (Nothing : x'rest) (Nothing : y'rest) =
      cmp (IndexSpec s'rest) x'rest y'rest
    -- always sort absent field value to last
    cmp _ (Nothing : _) (_       : _) = GT
    cmp _ (_       : _) (Nothing : _) = LT
    -- non-nil comparation, apply asc/desc translation
    cmp (IndexSpec ((_, asc) : s'rest)) (Just x'v : x'rest) (Just y'v : y'rest)
      = case compare x'v y'v of
        EQ -> cmp (IndexSpec s'rest) x'rest y'rest
        LT -> if asc then LT else GT
        GT -> if asc then GT else LT

extractIndexKey
  :: EdhProgState -> IndexSpec -> Object -> (IndexKey -> STM ()) -> STM ()
extractIndexKey !pgs spec@(IndexSpec !spec') !bo !exit =
  seqcontSTM (extractField . fst <$> spec') $ exit . IndexKey spec
 where
  extractField :: AttrKey -> (Maybe IdxKeyVal -> STM ()) -> STM ()
  extractField !k !exit' =
    lookupEdhObjAttr pgs bo k >>= \v -> edhIdxKeyVal pgs v exit'

edhValueOfIndexKey :: IndexKey -> EdhValue
edhValueOfIndexKey (IndexKey _ ikvs) = EdhTuple $ edhValOfIdxKey <$> ikvs


-- | Non-Unique Business Object Index
data BoIndex = BoIndex {
      spec'of'nou'idx :: !IndexSpec
      -- TODO find and use an effecient container with fast entry key update
      --      it's less optimal as using two maps here for now
    , tree'of'nou'idx :: !(TreeMap.Map IndexKey (Set.HashSet Object))
    , reverse'of'nou'idx :: !(Map.HashMap Object IndexKey)
  }

lookupBoIndex :: BoIndex -> [Maybe IdxKeyVal] -> STM EdhValue
lookupBoIndex (BoIndex !spec !tree _) !idxKeyVals =
  case TreeMap.lookup (IndexKey spec idxKeyVals) tree of
    Nothing   -> return EdhNil
    Just !bos -> return $ EdhTuple $ EdhObject <$> Set.toList bos

reindexBusObj
  :: EdhProgState -> BoIndex -> Object -> (BoIndex -> STM ()) -> STM ()
reindexBusObj !pgs boi@(BoIndex !spec !tree !rvrs) !bo !exit =
  extractIndexKey pgs spec bo $ \newKey -> exit boi
    { tree'of'nou'idx    = TreeMap.alter putBoIn newKey tree'
    , reverse'of'nou'idx = Map.insert bo newKey rvrs
    }
 where
  putBoIn :: Maybe (Set.HashSet Object) -> Maybe (Set.HashSet Object)
  putBoIn oldEntry = case oldEntry of
    Nothing       -> Just $ Set.singleton bo
    Just siblings -> Just $ Set.insert bo siblings
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.update (Just . Set.delete bo) oldKey tree

throwAwayIdxObj
  :: EdhProgState -> BoIndex -> Object -> (BoIndex -> STM ()) -> STM ()
throwAwayIdxObj _ boi@(BoIndex _ !tree !rvrs) !bo !exit = exit boi
  { tree'of'nou'idx    = tree'
  , reverse'of'nou'idx = Map.delete bo rvrs
  }
 where
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.update (Just . Set.delete bo) oldKey tree

listIdxGroups
  :: BoIndex
  -> Maybe [Maybe IdxKeyVal]
  -> Maybe [Maybe IdxKeyVal]
  -> STM [(IndexKey, EdhValue)]
listIdxGroups (BoIndex !spec !tree _) !minKeyVals !maxKeyVals =
  return $ (<$> seg) $ \(k, bos) -> (k, EdhTuple $ EdhObject <$> Set.toList bos)
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals

-- | host constructor BoIndex( indexSpec )
boiHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store
  -> (Dynamic -> STM ())  -- in-band data to be written to entity store
  -> STM ()
boiHostCtor !pgsCtor (ArgsPack !ctorArgs !ctorKwargs) !obs !ctorExit = do
  let !scope = contextScope $ edh'context pgsCtor
  parseIndexSpec pgsCtor ctorArgs $ \spec@(IndexSpec spec') -> do
    let specStr = T.pack $ show spec
        idxName = case Map.lookup (AttrByName "name") ctorKwargs of
          Nothing                 -> "<index>"
          Just (EdhString keyStr) -> keyStr
          Just v                  -> T.pack $ show v
    modifyTVar' obs $ Map.union $ Map.fromList
      [ (AttrByName "spec", EdhString specStr)
      , (AttrByName "keys", EdhTuple $ attrKeyValue . fst <$> spec')
      , (AttrByName "name", EdhString idxName)
      , ( AttrByName "__repr__"
        , EdhString $ "Index " <> idxName <> " " <> specStr
        )
      ]
    modifyTVar' obs . Map.union =<< Map.fromList <$> sequence
      [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
      | (nm, vc, hp, mthArgs) <-
        [ ( "<-"
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
          , boiGroupsProc
          , PackReceiver
            [ RecvArg "min" Nothing (Just edhNoneExpr)
            , RecvArg "max" Nothing (Just edhNoneExpr)
            ]
          )
        ]
      ]
    let boi = BoIndex spec TreeMap.empty Map.empty
    boiVar <- newTMVar boi
    ctorExit $ toDyn boiVar

 where

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
            throwEdhSTM pgs UsageError $ "bug: this is not a boi : " <> T.pack
              (show esd)
          Just (boiVar :: TMVar BoIndex) -> do
-- index update is expensive, use TMVar to avoid computation being retried
            boi <- takeTMVar boiVar
            let tryAct !pgs' !exit' = reindexBusObj pgs' boi bo $ \boi' -> do
                  putTMVar boiVar boi'
                  exitEdhSTM pgs' exit' nil
            edhCatchSTM pgs tryAct exit $ \_pgsThrower _exv _recover rethrow ->
              do
                void $ tryPutTMVar boiVar boi
                rethrow
    _ -> throwEdh UsageError "Invalid args to boiReindexProc"

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
            throwEdhSTM pgs UsageError $ "bug: this is not a boi : " <> T.pack
              (show esd)
          Just (boiVar :: TMVar BoIndex) -> do
-- index update is expensive, use TMVar to avoid computation being retried
            boi <- takeTMVar boiVar
            let tryAct !pgs' !exit' = throwAwayIdxObj pgs' boi bo $ \boi' -> do
                  putTMVar boiVar boi'
                  exitEdhSTM pgs' exit' nil
            edhCatchSTM pgs tryAct exit $ \_pgsThrower _exv _recover rethrow ->
              do
                void $ tryPutTMVar boiVar boi
                rethrow
    _ -> throwEdh UsageError "Invalid args to boiThrowAwayProc"

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
            throwEdhSTM pgs UsageError $ "bug: this is not a boi : " <> T.pack
              (show esd)
          Just (boiVar :: TMVar BoIndex) -> do
            boi <- readTMVar boiVar
            edhIdxKeyVals pgs keyValues $ \case
              Nothing          -> exitEdhSTM pgs exit nil
              Just !idxKeyVals -> do
                result <- lookupBoIndex boi idxKeyVals
                exitEdhSTM pgs exit result
    _ -> throwEdh UsageError "Invalid args to boiLookupProc"

  -- | host generator boi.groups( min=None, max=None )
  boiGroupsProc :: EdhProcedure
  boiGroupsProc !apk !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdh UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> do
        let
          yieldResults :: [(IndexKey, EdhValue)] -> STM ()
          yieldResults [] = exitEdhSTM pgs exit nil
          yieldResults ((ik, v) : rest) =
            runEdhProc pgs'
              $ iter'cb
                  (EdhArgsPack
                    (ArgsPack [edhValueOfIndexKey ik, noneNil v] Map.empty)
                  )
              $ \case
                  Left (pgsThrower, exv) ->
                    edhThrowSTM pgsThrower { edh'context = edh'context pgs } exv
                  Right EdhBreak         -> exitEdhSTM pgs exit nil
                  Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
                  _                      -> yieldResults rest
        case parseIdxRng apk of
          Left argsErr -> throwEdh UsageError $ argsErr <> " for boiGroupsProc"
          Right (minKey, maxKey) -> contEdhSTM $ do
            esd <- readTVar es
            case fromDynamic esd of
              Nothing ->
                throwEdhSTM pgs UsageError
                  $  "bug: this is not a boi : "
                  <> T.pack (show esd)
              Just (boiVar :: TMVar BoIndex) -> do
                boi <- readTMVar boiVar
                edhIdxKeyVals pgs minKey $ \minKeyVals ->
                  edhIdxKeyVals pgs maxKey $ \maxKeyVals -> do
                    result <- listIdxGroups boi minKeyVals maxKeyVals
                    yieldResults result


-- | Unique Business Object Index
data BuIndex = BuIndex {
      spec'of'uniq'idx :: !IndexSpec
      -- TODO find and use an effecient container with fast entry key update
      --      it's less optimal as using two maps here for now
    , tree'of'uniq'idx :: !(TreeMap.Map IndexKey Object)
    , reverse'of'uniq'idx :: !(Map.HashMap Object IndexKey)
  }

lookupBuIndex :: BuIndex -> [Maybe IdxKeyVal] -> STM EdhValue
lookupBuIndex (BuIndex !spec !tree _) !idxKeyVals =
  case TreeMap.lookup (IndexKey spec idxKeyVals) tree of
    Nothing  -> return EdhNil
    Just !bo -> return $ EdhObject bo

reindexUniqObj
  :: EdhProgState -> Text -> BuIndex -> Object -> (BuIndex -> STM ()) -> STM ()
reindexUniqObj !pgs idxName bui@(BuIndex !spec !tree !rvrs) !bo !exit =
  extractIndexKey pgs spec bo $ \newKey -> case TreeMap.lookup newKey tree' of
    Just _ ->
      throwEdhSTM pgs EdhException -- todo use db specific exception here ?
        $  "Violation of unique constraint on index: "
        <> idxName
        <> " "
        <> T.pack (show spec)
    Nothing -> exit bui { tree'of'uniq'idx    = TreeMap.insert newKey bo tree'
                        , reverse'of'uniq'idx = Map.insert bo newKey rvrs
                        }
 where
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.delete oldKey tree

throwAwayUniqObj
  :: BuIndex -> EdhProgState -> Object -> (BuIndex -> STM ()) -> STM ()
throwAwayUniqObj bui@(BuIndex _ !tree !rvrs) _ !bo !exit = exit bui
  { tree'of'uniq'idx    = tree'
  , reverse'of'uniq'idx = Map.delete bo rvrs
  }
 where
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.delete oldKey tree

listBuIndexRange
  :: BuIndex
  -> Maybe [Maybe IdxKeyVal]
  -> Maybe [Maybe IdxKeyVal]
  -> STM [(IndexKey, EdhValue)]
listBuIndexRange (BuIndex !spec !tree _) !minKeyVals !maxKeyVals =
  return $ (<$> seg) $ \(k, bo) -> (k, EdhObject bo)
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals

-- | host constructor BuIndex( indexSpec )
buiHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store
  -> (Dynamic -> STM ())  -- in-band data to be written to entity store
  -> STM ()
buiHostCtor !pgsCtor (ArgsPack !ctorArgs !ctorKwargs) !obs !ctorExit = do
  let !scope = contextScope $ edh'context pgsCtor
  parseIndexSpec pgsCtor ctorArgs $ \spec@(IndexSpec spec') -> do
    let specStr = T.pack $ show spec
        idxName = case Map.lookup (AttrByName "name") ctorKwargs of
          Nothing                 -> "<unique-index>"
          Just (EdhString keyStr) -> keyStr
          Just v                  -> T.pack $ show v
    modifyTVar' obs $ Map.union $ Map.fromList
      [ (AttrByName "spec", EdhString specStr)
      , (AttrByName "keys", EdhTuple $ attrKeyValue . fst <$> spec')
      , (AttrByName "name", EdhString idxName)
      , ( AttrByName "__repr__"
        , EdhString $ "UniqueIndex " <> idxName <> " " <> specStr
        )
      ]
    modifyTVar' obs . Map.union =<< Map.fromList <$> sequence
      [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
      | (nm, vc, hp, mthArgs) <-
        [ ( "<-"
          , EdhMethod
          , buiReindexProc
          , PackReceiver [RecvArg "bo" Nothing Nothing]
          )
        , ( "^*"
          , EdhMethod
          , buiThrowAwayProc
          , PackReceiver [RecvArg "bo" Nothing Nothing]
          )
        , ( "[]"
          , EdhMethod
          , buiLookupProc
          , PackReceiver [RecvArg "keyValues" Nothing Nothing]
          )
        , ( "range"
          , EdhGnrtor
          , buiRangeProc
          , PackReceiver
            [ RecvArg "min" Nothing (Just edhNoneExpr)
            , RecvArg "max" Nothing (Just edhNoneExpr)
            ]
          )
        ]
      ]
    let bui = BuIndex spec TreeMap.empty Map.empty
    buiVar <- newTMVar bui
    ctorExit $ toDyn buiVar

 where

  buiName :: STM Text
  buiName = Map.lookup (AttrByName "name") <$> readTVar obs >>= \case
    Nothing               -> return "<unique-index>"
    Just (EdhString name) -> return name
    Just v                -> return $ T.pack $ show v

  buiReindexProc :: EdhProcedure
  buiReindexProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs UsageError $ "bug: this is not a bui : " <> T.pack
              (show esd)
          Just (buiVar :: TMVar BuIndex) -> do
            idxName <- buiName
-- index update is expensive, use TMVar to avoid computation being retried
            bui     <- takeTMVar buiVar
            let tryAct !pgs' !exit' =
                  reindexUniqObj pgs' idxName bui bo $ \bui' -> do
                    putTMVar buiVar bui'
                    exitEdhSTM pgs' exit' nil
            edhCatchSTM pgs tryAct exit $ \_pgsThrower _exv _recover rethrow ->
              do
                void $ tryPutTMVar buiVar bui
                rethrow
    _ -> throwEdh UsageError "Invalid args to buiReindexProc"

  buiThrowAwayProc :: EdhProcedure
  buiThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs UsageError $ "bug: this is not a bui : " <> T.pack
              (show esd)
          Just (buiVar :: TMVar BuIndex) -> do
-- index update is expensive, use TMVar to avoid computation being retried
            bui <- takeTMVar buiVar
            let tryAct !pgs' !exit' = throwAwayUniqObj bui pgs' bo $ \bui' ->
                  do
                    putTMVar buiVar bui'
                    exitEdhSTM pgs' exit' nil
            edhCatchSTM pgs tryAct exit $ \_pgsThrower _exv _recover rethrow ->
              do
                void $ tryPutTMVar buiVar bui
                rethrow
    _ -> throwEdh UsageError "Invalid args to buiThrowAwayProc"

  buiLookupProc :: EdhProcedure
  buiLookupProc (ArgsPack !args !kwargs) !exit = case args of
    [keyValues] | Map.null kwargs -> do
      pgs <- ask
      let this = thisObject $ contextScope $ edh'context pgs
          es   = entity'store $ objEntity this
      contEdhSTM $ do
        esd <- readTVar es
        case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs UsageError $ "bug: this is not a bui : " <> T.pack
              (show esd)
          Just (buiVar :: TMVar BuIndex) -> do
            bui <- readTMVar buiVar
            edhIdxKeyVals pgs keyValues $ \case
              Nothing          -> exitEdhSTM pgs exit nil
              Just !idxKeyVals -> do
                result <- lookupBuIndex bui idxKeyVals
                exitEdhSTM pgs exit result
    _ -> throwEdh UsageError "Invalid args to buiLookupProc"

  -- | host generator bui.range( min=None, max=None )
  buiRangeProc :: EdhProcedure
  buiRangeProc !apk !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdh UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> do
        let
          yieldResults :: [(IndexKey, EdhValue)] -> STM ()
          yieldResults [] = exitEdhSTM pgs exit nil
          yieldResults ((ik, v) : rest) =
            runEdhProc pgs'
              $ iter'cb
                  (EdhArgsPack
                    (ArgsPack [edhValueOfIndexKey ik, noneNil v] Map.empty)
                  )
              $ \case
                  Left (pgsThrower, exv) ->
                    edhThrowSTM pgsThrower { edh'context = edh'context pgs } exv
                  Right EdhBreak         -> exitEdhSTM pgs exit nil
                  Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
                  _                      -> yieldResults rest
        case parseIdxRng apk of
          Left argsErr -> throwEdh UsageError $ argsErr <> " for buiRangeProc"
          Right (minKey, maxKey) -> contEdhSTM $ do
            esd <- readTVar es
            case fromDynamic esd of
              Nothing ->
                throwEdhSTM pgs UsageError
                  $  "bug: this is not a bui : "
                  <> T.pack (show esd)
              Just (buiVar :: TMVar BuIndex) -> do
                bui <- readTMVar buiVar
                edhIdxKeyVals pgs minKey $ \minKeyVals ->
                  edhIdxKeyVals pgs maxKey $ \maxKeyVals -> do
                    result <- listBuIndexRange bui minKeyVals maxKeyVals
                    yieldResults result


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


indexKeyRange
  :: IndexSpec
  -> TreeMap.Map IndexKey a
  -> Maybe [Maybe IdxKeyVal]
  -> Maybe [Maybe IdxKeyVal]
  -> [(IndexKey, a)]
indexKeyRange spec tree !minKeyVals !maxKeyVals = TI.foldRange
  (: [])
  (IndexKey spec <$> minKeyVals, IndexKey spec <$> maxKeyVals)
  tree

