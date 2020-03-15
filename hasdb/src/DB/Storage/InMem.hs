
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

import           Language.Edh.EHI


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

indexFieldSortOrderSpec :: EdhProgState -> Text -> STM Bool
indexFieldSortOrderSpec pgs ordSpec = case ordSpec of
  "ASC"        -> return True
  "DESC"       -> return False
  "ASCENDING"  -> return True
  "DESCENDING" -> return False
  "asc"        -> return True
  "desc"       -> return False
  "ascending"  -> return True
  "descending" -> return False
  _ ->
    throwEdhSTM pgs EvalError $ "Invalid index field sorting order: " <> ordSpec

parseIndexSpec :: EdhProgState -> [EdhValue] -> STM IndexSpec
parseIndexSpec pgs !args = do
  spec <- case args of
    [EdhExpr _ (AttrExpr (DirectRef (NamedAttr !attrName))) _] ->
      return [(AttrByName attrName, True)]
    [EdhExpr _ (InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (AttrExpr (DirectRef (NamedAttr ordSpec)))) _]
      -> do
        asc <- indexFieldSortOrderSpec pgs ordSpec
        return [(AttrByName attrName, asc)]
    [EdhExpr _ (InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (LitExpr (BoolLiteral !asc))) _]
      -> return [(AttrByName attrName, asc)]
    [EdhExpr _ (TupleExpr !fieldSpecs) _] ->
      sequence $ fieldFromExpr <$> fieldSpecs
    fieldSpecs -> sequence $ fieldFromVal <$> fieldSpecs
  when (null spec)
    $ throwEdhSTM pgs EvalError "Index specification can not be empty"
  return $ IndexSpec spec
 where
  fieldFromExpr :: Expr -> STM (AttrKey, AscSort)
  fieldFromExpr x = case x of
    AttrExpr (DirectRef (NamedAttr !attrName)) ->
      return (AttrByName attrName, True)
    InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (AttrExpr (DirectRef (NamedAttr ordSpec)))
      -> do
        asc <- indexFieldSortOrderSpec pgs ordSpec
        return (AttrByName attrName, asc)
    InfixExpr ":" (AttrExpr (DirectRef (NamedAttr !attrName))) (LitExpr (BoolLiteral !asc))
      -> return (AttrByName attrName, asc)
    TupleExpr [AttrExpr (DirectRef (NamedAttr !attrName)), AttrExpr (DirectRef (NamedAttr ordSpec))]
      -> do
        asc <- indexFieldSortOrderSpec pgs ordSpec
        return (AttrByName attrName, asc)
    TupleExpr [AttrExpr (DirectRef (NamedAttr !attrName)), LitExpr (BoolLiteral !asc)]
      -> return (AttrByName attrName, asc)
    _ -> throwEdhSTM pgs EvalError $ "Invalid index field spec: " <> T.pack
      (show x)
  fieldFromVal :: EdhValue -> STM (AttrKey, AscSort)
  fieldFromVal x = case x of
    EdhPair (EdhString !attrName) (EdhBool !asc) ->
      return (AttrByName attrName, asc)
    EdhTuple [EdhString !attrName, EdhBool !asc] ->
      return (AttrByName attrName, asc)
    EdhString !attrName -> return (AttrByName attrName, True)
    EdhPair (EdhString !attrName) (EdhString !ordSpec) -> do
      asc <- indexFieldSortOrderSpec pgs ordSpec
      return (AttrByName attrName, asc)
    EdhTuple [EdhString !attrName, EdhString !ordSpec] -> do
      asc <- indexFieldSortOrderSpec pgs ordSpec
      return (AttrByName attrName, asc)
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

edhIdxKeyVal :: EdhProgState -> EdhValue -> STM (Maybe IdxKeyVal)
edhIdxKeyVal _    EdhNil          = return Nothing
edhIdxKeyVal _    (EdhBool    v ) = return $ Just $ IdxBoolVal v
edhIdxKeyVal _    (EdhDecimal v ) = return $ Just $ IdxNumVal v
edhIdxKeyVal _    (EdhString  v ) = return $ Just $ IdxStrVal v
edhIdxKeyVal !pgs (EdhTuple   vs) = do
  ikvs <- sequence $ edhIdxKeyVal pgs <$> vs
  return $ Just $ IdxAggrVal ikvs
-- list is mutable, meaning can change without going through entity attr
-- update, don't support it so far
-- edhIdxKeyVal !pgs (EdhList (List _ vs')) = do
--   vs   <- readTVar vs'
--   ikvs <- sequence $ edhIdxKeyVal pgs <$> vs
--   return $ Just $ IdxAggrVal ikvs
edhIdxKeyVal !pgs !val =
  throwEdhSTM pgs EvalError $ "Invalid value type to be indexed: " <> T.pack
    (show $ edhTypeOf val)

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

extractIndexKey :: EdhProgState -> IndexSpec -> Object -> STM IndexKey
extractIndexKey !pgs spec@(IndexSpec !spec') !bo = do
  kd <- sequence $ extractField . fst <$> spec'
  return $ IndexKey spec kd
 where
  extractField :: AttrKey -> STM (Maybe IdxKeyVal)
  extractField !k = lookupEdhObjAttr pgs bo k >>= edhIdxKeyVal pgs


-- | Unique Business Object Index
data UniqBoIdx = UniqBoIdx {
      spec'of'uniq'idx :: !IndexSpec
      -- TODO find and use an effecient container with fast entry key update
      --      it's less optimal as using two maps here for now
    , tree'of'uniq'idx :: !(TreeMap.Map IndexKey Object)
    , reverse'of'uniq'idx :: !(Map.HashMap Object IndexKey)
  }

reindexUniqBusObj :: UniqBoIdx -> EdhProgState -> Object -> STM UniqBoIdx
reindexUniqBusObj uboi@(UniqBoIdx spec !tree !rvrs) !pgs !bo = do
  newKey <- extractIndexKey pgs spec bo
  case TreeMap.lookup newKey tree' of
    Just _ ->
      throwEdhSTM pgs EvalError
        $  "Violation of unique constraint on index: "
        <> T.pack (show spec)
    Nothing -> return uboi { tree'of'uniq'idx = TreeMap.insert newKey bo tree'
                           , reverse'of'uniq'idx = Map.insert bo newKey rvrs
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

reindexNouBusObj :: NouBoIdx -> EdhProgState -> Object -> STM NouBoIdx
reindexNouBusObj boi@(NouBoIdx !spec !tree !rvrs) !pgs !bo = do
  newKey <- extractIndexKey pgs spec bo
  return boi { tree'of'nou'idx    = TreeMap.alter putBoIn newKey tree'
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


data BoIndex = UniqueIndex !UniqBoIdx | NonUniqueIndex !NouBoIdx

indexSpecOf :: BoIndex -> IndexSpec
indexSpecOf (UniqueIndex    (UniqBoIdx spec _ _)) = spec
indexSpecOf (NonUniqueIndex (NouBoIdx  spec _ _)) = spec

reindexBusinessObject :: BoIndex -> EdhProgState -> Object -> STM BoIndex
reindexBusinessObject (UniqueIndex !boi) pgs bo =
  UniqueIndex <$> reindexUniqBusObj boi pgs bo
reindexBusinessObject (NonUniqueIndex !boi) pgs bo =
  NonUniqueIndex <$> reindexNouBusObj boi pgs bo

lookupBoIndex :: BoIndex -> [Maybe IdxKeyVal] -> STM EdhValue
lookupBoIndex (UniqueIndex (UniqBoIdx !spec !tree _)) !idxKeyVals =
  case TreeMap.lookup (IndexKey spec idxKeyVals) tree of
    Nothing  -> return EdhNil
    Just !bo -> return $ EdhObject bo
lookupBoIndex (NonUniqueIndex (NouBoIdx !spec !tree _)) !idxKeyVals =
  case TreeMap.lookup (IndexKey spec idxKeyVals) tree of
    Nothing   -> return EdhNil
    Just !bos -> return $ EdhTuple $ EdhObject <$> Set.toList bos


yieldOneEntity :: Int -> EdhGenrCaller -> STM ()
yieldOneEntity !delayMicros genr'caller@(!pgs', !iter'cb) = do
  let v = 1
  -- yield the nanosecond timestamp to iterator
  runEdhProg pgs' $ iter'cb (EdhDecimal v) $ \_ ->
    yieldOneEntity delayMicros genr'caller


