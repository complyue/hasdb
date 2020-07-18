
module DB.Storage.InMem where

import           Prelude
-- import           Debug.Trace

import           Control.Concurrent.STM

import           Control.Monad
-- import           Control.Monad.Reader

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import qualified Data.Map.Strict               as TreeMap
import qualified Data.HashSet                  as Set
import           Data.Unique
import           Data.Dynamic

import qualified DB.Storage.InMem.TreeIdx      as TI

import           Language.Edh.EHI


-- | Business Object Set
type BoSet = Set.HashSet Object

-- | host constructor BoSet()
bosCtor :: EdhHostCtor
-- implement bos objects as heavy entity based, i.e. use 'TMVar' to wrape the
-- entity store, as to avoid space and time waste on STM retries
bosCtor _ _ !ctorExit = newTMVar (Set.empty :: BoSet) >>= ctorExit . toDyn

bosMethods :: Unique -> EdhProgState -> STM [(AttrKey, EdhValue)]
bosMethods !classUniq !pgsModule = sequence
  [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp args
  | (nm, vc, hp, args) <-
    [ ("<-" , EdhMethod, bosAddProc      , PackReceiver [mandatoryArg "bo"])
    , ("^*" , EdhMethod, bosThrowAwayProc, PackReceiver [mandatoryArg "bo"])
    , ("all", EdhGnrtor, bosAllProc      , PackReceiver [])
    ]
  ]
 where
  !scope = contextScope $ edh'context pgsModule

  bosAddProc :: EdhProcedure
  bosAddProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      modifyEntityOfClass classUniq exit $ \ !pgs !bos !modExit ->
        modExit (Set.insert bo bos)
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to bosAddProc"

  bosThrowAwayProc :: EdhProcedure
  bosThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      modifyEntityOfClass classUniq exit $ \ !pgs !bos !modExit ->
        modExit (Set.delete bo bos)
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to bosThrowAwayProc"

  -- | host generator bos.all()
  bosAllProc :: EdhProcedure
  bosAllProc _ !exit = withEntityOfClass classUniq $ \ !pgs !bosVar ->
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdhSTM pgs UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) ->
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
        in
          readTMVar bosVar >>= yieldResults . Set.toList


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
  [EdhExpr _ (ArgsPackExpr !fieldSpecs) _] ->
    seqcontSTM (fieldFromArgSndr <$> fieldSpecs) doExit
  [EdhArgsPack (ArgsPack !fieldSpecs _)] ->
    seqcontSTM (fieldFromVal <$> fieldSpecs) doExit
  [EdhList (List _ !fsl)] -> do
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
  fieldFromArgSndr :: ArgSender -> ((AttrKey, AscSort) -> STM ()) -> STM ()
  fieldFromArgSndr !sndr !exit' = case sndr of
    SendPosArg !x -> fieldFromExpr x exit'
    _ -> throwEdhSTM pgs UsageError $ "Invalid index field spec: " <> T.pack
      (show sndr)
  fieldFromExpr :: Expr -> ((AttrKey, AscSort) -> STM ()) -> STM ()
  fieldFromExpr !x !exit' = case x of
    AttrExpr (DirectRef (NamedAttr !attrName)) ->
      exit' (AttrByName attrName, True)
    InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (AttrExpr (DirectRef (NamedAttr ordSpec)))
      -> indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (LitExpr (BoolLiteral !asc))
      -> exit' (AttrByName attrName, asc)
    ArgsPackExpr [SendPosArg (AttrExpr (DirectRef (NamedAttr !attrName))), SendPosArg (AttrExpr (DirectRef (NamedAttr ordSpec)))]
      -> indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    ArgsPackExpr [SendPosArg (AttrExpr (DirectRef (NamedAttr !attrName))), SendPosArg (LitExpr (BoolLiteral !asc))]
      -> exit' (AttrByName attrName, asc)
    _ -> throwEdhSTM pgs UsageError $ "Invalid index field spec: " <> T.pack
      (show x)
  fieldFromVal :: EdhValue -> ((AttrKey, AscSort) -> STM ()) -> STM ()
  fieldFromVal !x !exit' = case x of
    EdhPair (EdhString !attrName) (EdhBool !asc) ->
      exit' (AttrByName attrName, asc)
    EdhArgsPack (ArgsPack [EdhString !attrName, EdhBool !asc] _) ->
      exit' (AttrByName attrName, asc)
    EdhString !attrName -> exit' (AttrByName attrName, True)
    EdhPair (EdhString !attrName) (EdhString !ordSpec) ->
      indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    EdhArgsPack (ArgsPack [EdhString !attrName, EdhString !ordSpec] _) ->
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
edhIdxKeyVal !pgs (EdhArgsPack (ArgsPack !vs _)) !exit =
  seqcontSTM (edhIdxKeyVal pgs <$> vs) $ exit . Just . IdxAggrVal
-- list is mutable, meaning can change without going through entity attr
-- update, don't support it unless sth else is figured out
edhIdxKeyVal !pgs !val _ =
  throwEdhSTM pgs UsageError $ "Invalid value type to be indexed: " <> T.pack
    (edhTypeNameOf val)

edhIdxKeyVals
  :: EdhProgState -> EdhValue -> (Maybe [Maybe IdxKeyVal] -> STM ()) -> STM ()
edhIdxKeyVals _ EdhNil                        !exit = exit Nothing
edhIdxKeyVals _ (EdhArgsPack (ArgsPack [] _)) !exit = exit Nothing
edhIdxKeyVals pgs (EdhArgsPack (ArgsPack !kvs _)) !exit =
  seqcontSTM (edhIdxKeyVal pgs <$> kvs) $ exit . Just
edhIdxKeyVals pgs kv !exit = edhIdxKeyVal pgs kv $ \ikv -> exit $ Just [ikv]

edhValOfIdxKey :: Maybe IdxKeyVal -> EdhValue
edhValOfIdxKey Nothing               = nil
edhValOfIdxKey (Just (IdxBoolVal v)) = EdhBool v
edhValOfIdxKey (Just (IdxNumVal  v)) = EdhDecimal v
edhValOfIdxKey (Just (IdxStrVal  v)) = EdhString v
edhValOfIdxKey (Just (IdxAggrVal ikvs)) =
  EdhArgsPack $ ArgsPack (edhValOfIdxKey <$> ikvs) odEmpty


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
edhValueOfIndexKey (IndexKey _ ikvs) =
  EdhArgsPack $ ArgsPack (edhValOfIdxKey <$> ikvs) odEmpty


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
    Nothing -> return EdhNil
    Just !bos ->
      return $ EdhArgsPack $ (ArgsPack (EdhObject <$> Set.toList bos) odEmpty)

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
  return $ (<$> seg) $ \(k, bos) ->
    (k, EdhArgsPack $ ArgsPack (EdhObject <$> Set.toList bos) odEmpty)
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals

-- | host constructor BoIndex( indexSpec )
boiCtor :: EdhHostCtor
-- implement bos objects as heavy entity based, i.e. use 'TMVar' to wrape the
-- entity store, as to avoid space and time waste on STM retries
boiCtor !pgsCtor (ArgsPack !ctorArgs _) !ctorExit =
  parseIndexSpec pgsCtor ctorArgs $ \ !spec ->
    newTMVar (BoIndex spec TreeMap.empty Map.empty) >>= ctorExit . toDyn

boiMethods :: Unique -> EdhProgState -> STM [(AttrKey, EdhValue)]
boiMethods !classUniq !pgsModule = sequence
  [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
  | (nm, vc, hp, mthArgs) <-
    [ ("spec"    , EdhMethod, boiSpecProc     , PackReceiver [])
    , ("keys"    , EdhMethod, boiKeysProc     , PackReceiver [])
    , ("__repr__", EdhMethod, boiReprProc     , PackReceiver [])
    , ("<-", EdhMethod, boiReindexProc, PackReceiver [mandatoryArg "bo"])
    , ("^*", EdhMethod, boiThrowAwayProc, PackReceiver [mandatoryArg "bo"])
    , ("[]", EdhMethod, boiLookupProc, PackReceiver [mandatoryArg "keyValues"])
    , ( "groups"
      , EdhGnrtor
      , boiGroupsProc
      , PackReceiver
        [optionalArg "min" edhNoneExpr, optionalArg "max" edhNoneExpr]
      )
    ]
  ]
 where
  !scope = contextScope $ edh'context pgsModule

  boiSpecProc :: EdhProcedure
  boiSpecProc _ !exit = withEntityOfClass classUniq $ \ !pgs !boiVar -> do
    BoIndex (IndexSpec !spec) _ _ <- readTMVar boiVar
    exitEdhSTM pgs exit $ EdhString $ T.pack (show spec)

  boiKeysProc :: EdhProcedure
  boiKeysProc _ !exit = withEntityOfClass classUniq $ \ !pgs !boiVar -> do
    BoIndex (IndexSpec !spec) _ _ <- readTMVar boiVar
    exitEdhSTM pgs exit $ EdhArgsPack $ ArgsPack (attrKeyValue . fst <$> spec)
                                                 odEmpty

  boiReprProc :: EdhProcedure
  boiReprProc _ !exit = withEntityOfClass classUniq $ \ !pgs !boiVar -> do
    BoIndex (IndexSpec !spec) _ _ <- readTMVar boiVar
    exitEdhSTM pgs exit $ EdhString $ "BoIndex<" <> T.pack (show spec) <> ">"

  boiReindexProc :: EdhProcedure
  boiReindexProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      modifyEntityOfClass classUniq exit $ \ !pgs !boi !modExit ->
        reindexBusObj pgs boi bo $ \ !boi' ->
          modExit boi' $ EdhObject $ thatObject $ contextScope $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to boiReindexProc"

  boiThrowAwayProc :: EdhProcedure
  boiThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      modifyEntityOfClass classUniq exit $ \ !pgs !boi !modExit ->
        throwAwayIdxObj pgs boi bo
          $ \boi' ->
              modExit boi' $ EdhObject $ thatObject $ contextScope $ edh'context
                pgs
    _ -> throwEdh UsageError "Invalid args to boiThrowAwayProc"

  boiLookupProc :: EdhProcedure
  boiLookupProc (ArgsPack [keyValues] !kwargs) !exit | odNull kwargs =
    withEntityOfClass classUniq $ \ !pgs !boiVar -> do
      !boi <- readTMVar boiVar
      edhIdxKeyVals pgs keyValues $ \case
        Nothing          -> exitEdhSTM pgs exit nil
        Just !idxKeyVals -> do
          result <- lookupBoIndex boi idxKeyVals
          exitEdhSTM pgs exit result
  boiLookupProc _ _ = throwEdh UsageError "Invalid args to boiLookupProc"

  -- | host generator boi.groups( min=None, max=None )
  boiGroupsProc :: EdhProcedure
  boiGroupsProc !apk !exit = withEntityOfClass classUniq $ \ !pgs !boiVar ->
    readTMVar boiVar >>= \ !boi -> case generatorCaller $ edh'context pgs of
      Nothing -> throwEdhSTM pgs UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> do
        let
          yieldResults :: [(IndexKey, EdhValue)] -> STM ()
          yieldResults [] = exitEdhSTM pgs exit nil
          yieldResults ((ik, v) : rest) =
            runEdhProc pgs'
              $ iter'cb
                  (EdhArgsPack
                    (ArgsPack [edhValueOfIndexKey ik, noneNil v] odEmpty)
                  )
              $ \case
                  Left (pgsThrower, exv) ->
                    edhThrowSTM pgsThrower { edh'context = edh'context pgs } exv
                  Right EdhBreak         -> exitEdhSTM pgs exit nil
                  Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
                  _                      -> yieldResults rest
        case parseIdxRng apk of
          Left argsErr ->
            throwEdhSTM pgs UsageError $ argsErr <> " for boiGroupsProc"
          Right (minKey, maxKey) -> edhIdxKeyVals pgs minKey $ \minKeyVals ->
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
  :: EdhProgState -> BuIndex -> Object -> (BuIndex -> STM ()) -> STM ()
reindexUniqObj !pgs bui@(BuIndex !spec !tree !rvrs) !bo !exit =
  extractIndexKey pgs spec bo $ \newKey -> case TreeMap.lookup newKey tree' of
    Just _ ->
      throwEdhSTM pgs EdhException -- todo use db specific exception here ?
        $  "Violation of unique constraint on index: "
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
buiCtor :: EdhHostCtor
-- implement bos objects as heavy entity based, i.e. use 'TMVar' to wrape the
-- entity store, as to avoid space and time waste on STM retries
buiCtor !pgsCtor (ArgsPack !ctorArgs _) !ctorExit =
  parseIndexSpec pgsCtor ctorArgs $ \ !spec ->
    newTMVar (BuIndex spec TreeMap.empty Map.empty) >>= ctorExit . toDyn

buiMethods :: Unique -> EdhProgState -> STM [(AttrKey, EdhValue)]
buiMethods !classUniq !pgsModule = sequence
  [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
  | (nm, vc, hp, mthArgs) <-
    [ ("spec"    , EdhMethod, buiSpecProc     , PackReceiver [])
    , ("keys"    , EdhMethod, buiKeysProc     , PackReceiver [])
    , ("__repr__", EdhMethod, buiReprProc     , PackReceiver [])
    , ("<-", EdhMethod, buiReindexProc, PackReceiver [mandatoryArg "bo"])
    , ("^*", EdhMethod, buiThrowAwayProc, PackReceiver [mandatoryArg "bo"])
    , ("[]", EdhMethod, buiLookupProc, PackReceiver [mandatoryArg "keyValues"])
    , ( "range"
      , EdhGnrtor
      , buiRangeProc
      , PackReceiver
        [optionalArg "min" edhNoneExpr, optionalArg "max" edhNoneExpr]
      )
    ]
  ]
 where
  !scope = contextScope $ edh'context pgsModule

  buiSpecProc :: EdhProcedure
  buiSpecProc _ !exit = withEntityOfClass classUniq $ \ !pgs !buiVar -> do
    BuIndex (IndexSpec !spec) _ _ <- readTMVar buiVar
    exitEdhSTM pgs exit $ EdhString $ T.pack (show spec)

  buiKeysProc :: EdhProcedure
  buiKeysProc _ !exit = withEntityOfClass classUniq $ \ !pgs !buiVar -> do
    BuIndex (IndexSpec !spec) _ _ <- readTMVar buiVar
    exitEdhSTM pgs exit $ EdhArgsPack $ ArgsPack (attrKeyValue . fst <$> spec)
                                                 odEmpty

  buiReprProc :: EdhProcedure
  buiReprProc _ !exit = withEntityOfClass classUniq $ \ !pgs !buiVar -> do
    BuIndex (IndexSpec !spec) _ _ <- readTMVar buiVar
    exitEdhSTM pgs exit $ EdhString $ "BuIndex<" <> T.pack (show spec) <> ">"

  buiReindexProc :: EdhProcedure
  buiReindexProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      modifyEntityOfClass classUniq exit $ \ !pgs !bui !modExit ->
        reindexUniqObj pgs bui bo
          $ \bui' ->
              modExit bui' $ EdhObject $ thatObject $ contextScope $ edh'context
                pgs
    _ -> throwEdh UsageError "Invalid args to buiReindexProc"

  buiThrowAwayProc :: EdhProcedure
  buiThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      modifyEntityOfClass classUniq exit $ \ !pgs !bui !modExit ->
        throwAwayUniqObj bui pgs bo
          $ \bui' ->
              modExit bui' $ EdhObject $ thatObject $ contextScope $ edh'context
                pgs
    _ -> throwEdh UsageError "Invalid args to buiThrowAwayProc"

  buiLookupProc :: EdhProcedure
  buiLookupProc (ArgsPack [keyValues] !kwargs) !exit | odNull kwargs =
    withEntityOfClass classUniq $ \ !pgs !buiVar -> do
      !bui <- readTMVar buiVar
      edhIdxKeyVals pgs keyValues $ \case
        Nothing          -> exitEdhSTM pgs exit nil
        Just !idxKeyVals -> do
          result <- lookupBuIndex bui idxKeyVals
          exitEdhSTM pgs exit result
  buiLookupProc _ _ = throwEdh UsageError "Invalid args to buiLookupProc"

  -- | host generator bui.range( min=None, max=None )
  buiRangeProc :: EdhProcedure
  buiRangeProc !apk !exit = withEntityOfClass classUniq $ \ !pgs !buiVar ->
    readTMVar buiVar >>= \ !bui -> case generatorCaller $ edh'context pgs of
      Nothing -> throwEdhSTM pgs UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> do
        let
          yieldResults :: [(IndexKey, EdhValue)] -> STM ()
          yieldResults [] = exitEdhSTM pgs exit nil
          yieldResults ((ik, v) : rest) =
            runEdhProc pgs'
              $ iter'cb
                  (EdhArgsPack
                    (ArgsPack [edhValueOfIndexKey ik, noneNil v] odEmpty)
                  )
              $ \case
                  Left (pgsThrower, exv) ->
                    edhThrowSTM pgsThrower { edh'context = edh'context pgs } exv
                  Right EdhBreak         -> exitEdhSTM pgs exit nil
                  Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
                  _                      -> yieldResults rest
        case parseIdxRng apk of
          Left argsErr ->
            throwEdhSTM pgs UsageError $ argsErr <> " for buiRangeProc"
          Right (minKey, maxKey) -> edhIdxKeyVals pgs minKey $ \minKeyVals ->
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

