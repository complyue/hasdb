
module DB.Storage.InMem where

import           Prelude
-- import           Debug.Trace

import           Control.Monad
import           Control.Concurrent.STM

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import qualified Data.Map.Strict               as TreeMap
import qualified Data.HashSet                  as Set

import qualified DB.Storage.InMem.TreeIdx      as TI

import           Language.Edh.EHI


type BoSet = Set.HashSet Object


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
    throwEdhSTM pgs EvalError $ "Invalid index field sorting order: " <> ordSpec


parseIndexSpec :: EdhProgState -> [EdhValue] -> (IndexSpec -> STM ()) -> STM ()
parseIndexSpec pgs !args !exit = do
  case args of
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
    _ ->
      throwEdhSTM pgs EvalError $ "Invalid index specification: " <> T.pack
        (show args)
 where
  doExit :: [(AttrKey, AscSort)] -> STM ()
  doExit !spec = do
    when (null spec)
      $ throwEdhSTM pgs EvalError "Index specification can not be empty"
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
    _ -> throwEdhSTM pgs EvalError $ "Invalid index field spec: " <> T.pack
      (show x)
  fieldFromVal :: EdhValue -> ((AttrKey, AscSort) -> STM ()) -> STM ()
  fieldFromVal !x !exit' = case x of
    EdhPair (EdhString !attrName) (EdhBool !asc) ->
      exit' (AttrByName attrName, asc)
    EdhTuple [EdhString !attrName, EdhBool !asc] ->
      exit' (AttrByName attrName, asc)
    EdhString !attrName -> exit' (AttrByName attrName, True)
    EdhPair (EdhString !attrName) (EdhString !ordSpec) -> do
      indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    EdhTuple [EdhString !attrName, EdhString !ordSpec] -> do
      indexFieldSortOrderSpec pgs ordSpec
        $ \asc -> exit' (AttrByName attrName, asc)
    _ -> throwEdhSTM pgs EvalError $ "Invalid index field spec: " <> T.pack
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
  throwEdhSTM pgs EvalError $ "Invalid value type to be indexed: " <> T.pack
    (show $ edhTypeOf val)

edhIdxKeyVals
  :: EdhProgState -> EdhValue -> ((Maybe [Maybe IdxKeyVal]) -> STM ()) -> STM ()
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
  extractField :: AttrKey -> ((Maybe IdxKeyVal) -> STM ()) -> STM ()
  extractField !k !exit' =
    lookupEdhObjAttr pgs bo k >>= \v -> edhIdxKeyVal pgs v exit'

edhValueOfIndexKey :: IndexKey -> EdhValue
edhValueOfIndexKey (IndexKey _ ikvs) = EdhTuple $ edhValOfIdxKey <$> ikvs


-- | Unique Business Object Index
data UniqBoIdx = UniqBoIdx {
      spec'of'uniq'idx :: !IndexSpec
      -- TODO find and use an effecient container with fast entry key update
      --      it's less optimal as using two maps here for now
    , tree'of'uniq'idx :: !(TreeMap.Map IndexKey Object)
    , reverse'of'uniq'idx :: !(Map.HashMap Object IndexKey)
  }

reindexUniqBusObj
  :: Text
  -> UniqBoIdx
  -> EdhProgState
  -> Object
  -> (UniqBoIdx -> STM ())
  -> STM ()
reindexUniqBusObj idxName uboi@(UniqBoIdx !spec !tree !rvrs) !pgs !bo !exit =
  extractIndexKey pgs spec bo $ \newKey -> case TreeMap.lookup newKey tree' of
    Just _ ->
      throwEdhSTM pgs EvalError
        $  "Violation of unique constraint on index: "
        <> idxName
        <> " "
        <> T.pack (show spec)
    Nothing -> exit uboi { tree'of'uniq'idx    = TreeMap.insert newKey bo tree'
                         , reverse'of'uniq'idx = Map.insert bo newKey rvrs
                         }
 where
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.delete oldKey tree

throwAwayUniqIdxObj
  :: UniqBoIdx -> EdhProgState -> Object -> (UniqBoIdx -> STM ()) -> STM ()
throwAwayUniqIdxObj uboi@(UniqBoIdx _ !tree !rvrs) _ !bo !exit = exit uboi
  { tree'of'uniq'idx    = tree'
  , reverse'of'uniq'idx = Map.delete bo rvrs
  }
 where
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.delete oldKey tree


-- | Non-Unique Business Object Index
data NouBoIdx = NouBoIdx {
      spec'of'nou'idx :: !IndexSpec
      -- TODO find and use an effecient container with fast entry key update
      --      it's less optimal as using two maps here for now
    , tree'of'nou'idx :: !(TreeMap.Map IndexKey (Set.HashSet Object))
    , reverse'of'nou'idx :: !(Map.HashMap Object IndexKey)
  }

reindexNouBusObj
  :: Text
  -> NouBoIdx
  -> EdhProgState
  -> Object
  -> (NouBoIdx -> STM ())
  -> STM ()
reindexNouBusObj _idxName boi@(NouBoIdx !spec !tree !rvrs) !pgs !bo !exit =
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

throwAwayNouIdxObj
  :: NouBoIdx -> EdhProgState -> Object -> (NouBoIdx -> STM ()) -> STM ()
throwAwayNouIdxObj boi@(NouBoIdx _ !tree !rvrs) _ !bo !exit = exit boi
  { tree'of'nou'idx    = tree'
  , reverse'of'nou'idx = Map.delete bo rvrs
  }
 where
  tree' = case Map.lookup bo rvrs of
    Nothing     -> tree
    Just oldKey -> TreeMap.update (Just . Set.delete bo) oldKey tree


data BoIndex = UniqueIndex !UniqBoIdx | NonUniqueIndex !NouBoIdx

indexSpecOf :: BoIndex -> IndexSpec
indexSpecOf (UniqueIndex    (UniqBoIdx spec _ _)) = spec
indexSpecOf (NonUniqueIndex (NouBoIdx  spec _ _)) = spec


reindexBusinessObject
  :: Text -> BoIndex -> EdhProgState -> Object -> (BoIndex -> STM ()) -> STM ()
reindexBusinessObject idxName (UniqueIndex !boi) pgs bo !exit =
  reindexUniqBusObj idxName boi pgs bo $ exit . UniqueIndex
reindexBusinessObject idxName (NonUniqueIndex !boi) pgs bo !exit =
  reindexNouBusObj idxName boi pgs bo $ exit . NonUniqueIndex

throwAwayIndexedObject
  :: BoIndex -> EdhProgState -> Object -> (BoIndex -> STM ()) -> STM ()
throwAwayIndexedObject (UniqueIndex !boi) pgs bo !exit =
  throwAwayUniqIdxObj boi pgs bo $ exit . UniqueIndex
throwAwayIndexedObject (NonUniqueIndex !boi) pgs bo !exit =
  throwAwayNouIdxObj boi pgs bo $ exit . NonUniqueIndex


lookupBoIndex :: BoIndex -> [Maybe IdxKeyVal] -> STM EdhValue
lookupBoIndex (UniqueIndex (UniqBoIdx !spec !tree _)) !idxKeyVals =
  case TreeMap.lookup (IndexKey spec idxKeyVals) tree of
    Nothing  -> return EdhNil
    Just !bo -> return $ EdhObject bo
lookupBoIndex (NonUniqueIndex (NouBoIdx !spec !tree _)) !idxKeyVals =
  case TreeMap.lookup (IndexKey spec idxKeyVals) tree of
    Nothing   -> return EdhNil
    Just !bos -> return $ EdhTuple $ EdhObject <$> Set.toList bos


listBoIndexGroups
  :: BoIndex
  -> Maybe [Maybe IdxKeyVal]
  -> Maybe [Maybe IdxKeyVal]
  -> STM [(IndexKey, EdhValue)]
listBoIndexGroups (UniqueIndex (UniqBoIdx !spec !tree _)) !minKeyVals !maxKeyVals
  = return $ (<$> seg) $ \(k, bo) -> (k, EdhTuple [EdhObject bo])
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals
listBoIndexGroups (NonUniqueIndex (NouBoIdx !spec !tree _)) !minKeyVals !maxKeyVals
  = return $ (<$> seg) $ \(k, bos) ->
    (k, EdhTuple $ EdhObject <$> Set.toList bos)
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals

listBoIndexRange
  :: BoIndex
  -> Maybe [Maybe IdxKeyVal]
  -> Maybe [Maybe IdxKeyVal]
  -> STM [(IndexKey, EdhValue)]
listBoIndexRange (UniqueIndex (UniqBoIdx !spec !tree _)) !minKeyVals !maxKeyVals
  = return $ (<$> seg) $ \(k, bo) -> (k, EdhObject bo)
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals
listBoIndexRange (NonUniqueIndex (NouBoIdx !spec !tree _)) !minKeyVals !maxKeyVals
  = return $ (`concatMap` seg) $ \(k, bos) ->
    [ (k, EdhObject bo) | bo <- Set.toList bos ]
  where seg = indexKeyRange spec tree minKeyVals maxKeyVals


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

