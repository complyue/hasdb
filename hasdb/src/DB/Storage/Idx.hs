
module DB.Storage.Idx where

import           Prelude
-- import           Debug.Trace

import           Control.Concurrent.STM

import           Control.Monad
-- import           Control.Monad.Reader

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import           Data.Array.MArray

import           Control.Concurrent.STM.TSkipList.Internal
                                                ( TSkipList(..)
                                                , Node(..)
                                                , Traversal(..)
                                                , traverseSL
                                                )

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


indexScan
  :: IndexSpec
  -> Maybe [Maybe IdxKeyVal]
  -> Maybe [Maybe IdxKeyVal]
  -> TSkipList IndexKey a
  -> ((IndexKey, a) -> STM () -> STM ())
  -> STM ()
  -> STM ()
indexScan spec !minKey !maxKey !tsl !cont !exit = case minKey of
  Nothing -> case maxKey of
    Nothing          -> scan (listHead tsl)
    Just !maxKeyVals -> scanUpTo (IndexKey spec maxKeyVals) (listHead tsl)
  Just !minKeyVals -> locateNodeGE (IndexKey spec minKeyVals) tsl >>= \case
    Nil                   -> exit
    Node !k !var !fwdPtrs -> readTVar var >>= \ !val ->
      cont (k, val) $ case maxKey of
        Nothing          -> scan fwdPtrs
        Just !maxKeyVals -> scanUpTo (IndexKey spec maxKeyVals) fwdPtrs
 where
  scan !fwdPtrs = readArray fwdPtrs 1 >>= \case
    Nil -> exit
    Node k var fwdPtrs' ->
      readTVar var >>= \ !val -> cont (k, val) $ scan fwdPtrs'
  scanUpTo !kMax !fwdPtrs = readArray fwdPtrs 1 >>= \case
    Nil                 -> exit
    Node k var fwdPtrs' -> case compare k kMax of
      GT -> exit
      _  -> readTVar var >>= \ !val -> cont (k, val) $ scanUpTo kMax fwdPtrs'


locateNodeGE :: (Ord k) => k -> TSkipList k a -> STM (Node k a)
locateNodeGE k tskip = lookupAcc (listHead tskip) =<< readTVar (curLevel tskip)
 where
  lookupAcc fwdPtrs lvl = do
    let moveDown succNode level =
          if level <= 1 then return succNode else lookupAcc fwdPtrs (level - 1)
    let moveRight succNode = lookupAcc (forwardPtrs succNode)
    let onFound succNode _ = return succNode
    let travStrategy = Traversal { onLT          = moveDown
                                 , onGT          = moveRight
                                 , onEQ          = onFound
                                 , onNil         = moveDown
                                 , onSuccSuccNil = Nothing
                                 }
    traverseSL k fwdPtrs lvl travStrategy Nil
