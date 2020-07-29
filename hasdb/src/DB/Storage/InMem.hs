
module DB.Storage.InMem where

import           Prelude
-- import           Debug.Trace

import           Control.Concurrent.STM

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as HashMap
import           Data.Unique
import           Data.Dynamic

import qualified ListT                         as ListT

import qualified StmContainers.Map             as Map
import qualified StmContainers.Set             as Set

import           Control.Concurrent.STM.TSkipList.Internal
                                                ( TSkipList )
import qualified Control.Concurrent.STM.TSkipList
                                               as TSL

import           DB.Storage.Idx

import           Language.Edh.EHI


-- | Business Object Set
type BoSet = Set.Set Object

-- | host constructor BoSet()
bosCtor :: EdhHostCtor
bosCtor _ _ !ctorExit = do
  (bos :: BoSet) <- Set.new
  ctorExit $ toDyn bos

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
      withEntityOfClass classUniq $ \ !pgs !bos -> do
        Set.insert bo bos
        exitEdhSTM pgs exit
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to bosAddProc"

  bosThrowAwayProc :: EdhProcedure
  bosThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      withEntityOfClass classUniq $ \ !pgs !bos -> do
        Set.delete bo bos
        exitEdhSTM pgs exit
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to bosThrowAwayProc"

  -- | host generator bos.all()
  bosAllProc :: EdhProcedure
  bosAllProc _ !exit = withEntityOfClass classUniq $ \ !pgs (bos :: BoSet) ->
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
          ListT.toList (Set.listT bos) >>= yieldResults


-- | Non-Unique Business Object Index
data BoIndex = BoIndex {
      spec'of'nou'idx :: !IndexSpec
      -- todo specialize fast key update, taking advantage that new key values
      --      tend to be near by the old ones
    , tree'of'nou'idx :: !(TSkipList IndexKey BoSet)
    , reverse'of'nou'idx :: !(Map.Map Object IndexKey)
  }

lookupBoIndex :: BoIndex -> [Maybe IdxKeyVal] -> STM EdhValue
lookupBoIndex (BoIndex !spec !tsl _) !idxKeyVals =
  TSL.lookup (IndexKey spec idxKeyVals) tsl >>= \case
    Nothing   -> return EdhNil
    Just !bos -> do
      (bol :: [Object]) <- ListT.toList $ Set.listT bos
      return $ EdhArgsPack $ (ArgsPack (EdhObject <$> bol) odEmpty)

reindexBusObj :: EdhProgState -> BoIndex -> Object -> STM () -> STM ()
reindexBusObj !pgs (BoIndex !spec !tsl !rvrs) !bo !exit =
  extractIndexKey pgs spec bo $ \ !newKey -> do
    Map.lookup bo rvrs >>= \case
      Nothing      -> pure ()
      Just !oldKey -> TSL.lookup oldKey tsl >>= \case
        Nothing   -> pure ()
        Just !bos -> Set.delete bo bos

    bos <- TSL.lookup newKey tsl >>= \case
      Nothing -> do
        (bos :: BoSet) <- Set.new
        TSL.insert newKey bos tsl
        return bos
      Just !bos -> return bos
    Set.insert bo bos
    exit

throwAwayIdxObj :: EdhProgState -> BoIndex -> Object -> STM () -> STM ()
throwAwayIdxObj _ (BoIndex _ !tsl !rvrs) !bo !exit = do
  Map.lookup bo rvrs >>= \case
    Nothing      -> return ()
    Just !oldKey -> TSL.lookup oldKey tsl >>= \case
      Nothing   -> return ()
      Just !bos -> Set.delete bo bos
  exit


-- | host constructor BoIndex( indexSpec )
boiCtor :: EdhHostCtor
-- implement bos objects as heavy entity based, i.e. use 'TMVar' to wrape the
-- entity store, as to avoid space and time waste on STM retries
boiCtor !pgsCtor (ArgsPack !ctorArgs _) !ctorExit =
  parseIndexSpec pgsCtor ctorArgs $ \ !spec -> do
    -- use larger number of levels, bias read performance over write/mod/space
    !tsl  <- TSL.new' 0.5 31
    !rvrs <- Map.new
    ctorExit $ toDyn $ BoIndex spec tsl rvrs

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
  boiSpecProc _ !exit =
    withEntityOfClass classUniq $ \ !pgs (BoIndex (IndexSpec !spec) _ _) ->
      exitEdhSTM pgs exit $ EdhString $ T.pack (show spec)

  boiKeysProc :: EdhProcedure
  boiKeysProc _ !exit =
    withEntityOfClass classUniq $ \ !pgs (BoIndex (IndexSpec !spec) _ _) ->
      exitEdhSTM pgs exit $ EdhArgsPack $ ArgsPack
        (attrKeyValue . fst <$> spec)
        odEmpty

  boiReprProc :: EdhProcedure
  boiReprProc _ !exit =
    withEntityOfClass classUniq $ \ !pgs (BoIndex (IndexSpec !spec) _ _) ->
      exitEdhSTM pgs exit $ EdhString $ "BoIndex<" <> T.pack (show spec) <> ">"

  boiReindexProc :: EdhProcedure
  boiReindexProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      withEntityOfClass classUniq $ \ !pgs !boi ->
        reindexBusObj pgs boi bo
          $ exitEdhSTM pgs exit
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to boiReindexProc"

  boiThrowAwayProc :: EdhProcedure
  boiThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      withEntityOfClass classUniq $ \ !pgs !boi ->
        throwAwayIdxObj pgs boi bo
          $ exitEdhSTM pgs exit
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to boiThrowAwayProc"

  boiLookupProc :: EdhProcedure
  boiLookupProc (ArgsPack [keyValues] !kwargs) !exit | odNull kwargs =
    withEntityOfClass classUniq $ \ !pgs !boi ->
      edhIdxKeyVals pgs keyValues $ \case
        Nothing          -> exitEdhSTM pgs exit nil
        Just !idxKeyVals -> do
          result <- lookupBoIndex boi idxKeyVals
          exitEdhSTM pgs exit result
  boiLookupProc _ _ = throwEdh UsageError "Invalid args to boiLookupProc"

  -- | host generator boi.groups( min=None, max=None )
  boiGroupsProc :: EdhProcedure
  boiGroupsProc !apk !exit =
    withEntityOfClass classUniq $ \ !pgs (BoIndex !spec !tsl _) ->
      case generatorCaller $ edh'context pgs of
        Nothing -> throwEdhSTM pgs UsageError "Can only be called as generator"
        Just (!pgs', !iter'cb) -> do
          let yieldOne :: (IndexKey, BoSet) -> STM () -> STM ()
              yieldOne (!ik, !bos) !next = do
                !bol <- ListT.toList $ Set.listT bos
                runEdhProc pgs'
                  $ iter'cb
                      (EdhArgsPack $ ArgsPack
                        [ edhValueOfIndexKey ik
                        , EdhArgsPack $ ArgsPack (EdhObject <$> bol) odEmpty
                        ]
                        odEmpty
                      )
                  $ \case
                      Left (pgsThrower, exv) -> edhThrowSTM
                        pgsThrower { edh'context = edh'context pgs }
                        exv
                      Right EdhBreak         -> exitEdhSTM pgs exit nil
                      Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
                      _                      -> next
          case parseIdxRng apk of
            Left argsErr ->
              throwEdhSTM pgs UsageError $ argsErr <> " for boiGroupsProc"
            Right (minKey, maxKey) ->
              edhIdxKeyVals pgs minKey $ \ !minKeyVals ->
                edhIdxKeyVals pgs maxKey $ \ !maxKeyVals ->
                  indexScan spec minKeyVals maxKeyVals tsl yieldOne
                    $ exitEdhSTM pgs exit nil


-- | Unique Business Object Index
data BuIndex = BuIndex {
      spec'of'uniq'idx :: !IndexSpec
      -- todo specialize fast key update, taking advantage that new key values
      --      tend to be near by the old ones
    , tree'of'uniq'idx :: !(TSkipList IndexKey Object)
    , reverse'of'uniq'idx :: !(Map.Map Object IndexKey)
  }

lookupBuIndex :: BuIndex -> [Maybe IdxKeyVal] -> STM EdhValue
lookupBuIndex (BuIndex !spec !tsl _) !idxKeyVals =
  TSL.lookup (IndexKey spec idxKeyVals) tsl >>= \case
    Nothing  -> return EdhNil
    Just !bo -> return $ EdhObject bo

reindexUniqObj :: EdhProgState -> BuIndex -> Object -> STM () -> STM ()
reindexUniqObj !pgs (BuIndex !spec !tsl !rvrs) !bo !exit = do
  Map.lookup bo rvrs >>= \case
    Nothing      -> pure ()
    Just !oldKey -> TSL.delete oldKey tsl
  extractIndexKey pgs spec bo $ \ !newKey -> do
    Map.insert newKey bo rvrs
    TSL.lookup newKey tsl >>= \case
      Just _ ->
        throwEdhSTM pgs EdhException -- todo use db specific exception here ?
          $  "Violation of unique constraint on index: "
          <> T.pack (show spec)
      Nothing -> do
        TSL.insert newKey bo tsl
        exit

throwAwayUniqObj :: BuIndex -> EdhProgState -> Object -> STM () -> STM ()
throwAwayUniqObj (BuIndex _ !tsl !rvrs) _ !bo !exit = do
  Map.lookup bo rvrs >>= \case
    Nothing      -> pure ()
    Just !oldKey -> TSL.delete oldKey tsl
  exit


-- | host constructor BuIndex( indexSpec )
buiCtor :: EdhHostCtor
-- implement bos objects as heavy entity based, i.e. use 'TMVar' to wrape the
-- entity store, as to avoid space and time waste on STM retries
buiCtor !pgsCtor (ArgsPack !ctorArgs _) !ctorExit =
  parseIndexSpec pgsCtor ctorArgs $ \ !spec -> do
    -- use larger number of levels, bias read performance over write/mod/space
    !tsl  <- TSL.new' 0.5 31
    !rvrs <- Map.new
    ctorExit $ toDyn $ BuIndex spec tsl rvrs

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
  buiSpecProc _ !exit =
    withEntityOfClass classUniq $ \ !pgs (BuIndex (IndexSpec !spec) _ _) ->
      exitEdhSTM pgs exit $ EdhString $ T.pack (show spec)

  buiKeysProc :: EdhProcedure
  buiKeysProc _ !exit =
    withEntityOfClass classUniq $ \ !pgs (BuIndex (IndexSpec !spec) _ _) ->
      exitEdhSTM pgs exit $ EdhArgsPack $ ArgsPack
        (attrKeyValue . fst <$> spec)
        odEmpty

  buiReprProc :: EdhProcedure
  buiReprProc _ !exit =
    withEntityOfClass classUniq $ \ !pgs (BuIndex (IndexSpec !spec) _ _) ->
      exitEdhSTM pgs exit $ EdhString $ "BuIndex<" <> T.pack (show spec) <> ">"

  buiReindexProc :: EdhProcedure
  buiReindexProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      withEntityOfClass classUniq $ \ !pgs !bui ->
        reindexUniqObj pgs bui bo
          $ exitEdhSTM pgs exit
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to buiReindexProc"

  buiThrowAwayProc :: EdhProcedure
  buiThrowAwayProc (ArgsPack !args !kwargs) !exit = case args of
    [EdhObject !bo] | odNull kwargs ->
      withEntityOfClass classUniq $ \ !pgs !bui ->
        throwAwayUniqObj bui pgs bo
          $ exitEdhSTM pgs exit
          $ EdhObject
          $ thatObject
          $ contextScope
          $ edh'context pgs
    _ -> throwEdh UsageError "Invalid args to buiThrowAwayProc"

  buiLookupProc :: EdhProcedure
  buiLookupProc (ArgsPack [keyValues] !kwargs) !exit | odNull kwargs =
    withEntityOfClass classUniq $ \ !pgs !bui ->
      edhIdxKeyVals pgs keyValues $ \case
        Nothing          -> exitEdhSTM pgs exit nil
        Just !idxKeyVals -> do
          result <- lookupBuIndex bui idxKeyVals
          exitEdhSTM pgs exit result
  buiLookupProc _ _ = throwEdh UsageError "Invalid args to buiLookupProc"

  -- | host generator bui.range( min=None, max=None )
  buiRangeProc :: EdhProcedure
  buiRangeProc !apk !exit =
    withEntityOfClass classUniq $ \ !pgs (BuIndex !spec !tsl _) ->
      case generatorCaller $ edh'context pgs of
        Nothing -> throwEdhSTM pgs UsageError "Can only be called as generator"
        Just (!pgs', !iter'cb) -> do
          let
            yieldOne :: (IndexKey, Object) -> STM () -> STM ()
            yieldOne (!ik, !bo) !next =
              runEdhProc pgs'
                $ iter'cb
                    (EdhArgsPack
                      (ArgsPack [edhValueOfIndexKey ik, EdhObject bo] odEmpty)
                    )
                $ \case
                    Left (pgsThrower, exv) -> edhThrowSTM
                      pgsThrower { edh'context = edh'context pgs }
                      exv
                    Right EdhBreak         -> exitEdhSTM pgs exit nil
                    Right (EdhReturn !rtn) -> exitEdhSTM pgs exit rtn
                    _                      -> next
          case parseIdxRng apk of
            Left argsErr ->
              throwEdhSTM pgs UsageError $ argsErr <> " for buiRangeProc"
            Right (minKey, maxKey) ->
              edhIdxKeyVals pgs minKey $ \ !minKeyVals ->
                edhIdxKeyVals pgs maxKey $ \ !maxKeyVals ->
                  indexScan spec minKeyVals maxKeyVals tsl yieldOne
                    $ exitEdhSTM pgs exit nil


type IdxRng = (EdhValue, EdhValue)
parseIdxRng :: ArgsPack -> Either Text IdxRng
parseIdxRng =
  parseArgsPack (nil, nil)
    $ ArgsPackParser
        [ \v (_, argMax) -> Right (v, argMax)
        , \v (argMin, _) -> Right (argMin, v)
        ]
    $ HashMap.fromList
        [ ("min", \v (_, argMax) -> Right (v, argMax))
        , ("max", \v (argMin, _) -> Right (argMin, v))
        ]

