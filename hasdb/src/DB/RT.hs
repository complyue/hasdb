
module DB.RT where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader
-- import           Control.Concurrent
import           Control.Concurrent.STM

import           Data.List.NonEmpty             ( NonEmpty(..) )
import qualified Data.List.NonEmpty            as NE
import qualified Data.HashMap.Strict           as Map
import qualified Data.Text                     as T

import qualified Data.Lossless.Decimal         as D
import           Language.Edh.EHI

import           DB.Storage.DataDir


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
  classNameOf (EdhClass (ProcDefi _ _ pd)) = EdhString $ procedureName pd
  classNameOf (EdhObject !obj) =
    EdhString $ procedureName $ procedure'decl $ objClass obj
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
                        $ callEdhMethod bo mth'proc (ArgsPack [] Map.empty) id
                        $ \_ -> contEdhSTM $ exitEdhSTM pgs exit $ EdhObject bo
                    !badMth ->
                      throwEdhSTM pgs EvalError
                        $  "Invalid __db_init__() method type: "
                        <> T.pack (edhTypeNameOf badMth)
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
      | Map.null kwargs
      -> contEdhSTM
        $ edhPerformIO
        -- not to use `unsafeIOToSTM` here, despite it being retry prone,
        -- nested `atomically` is particularly prohibited.
            pgs
            (streamEdhReprToDisk (edh'context pgs)
                                 persistOutlet
                                 (T.unpack dataFileFolder)
                                 sinkBaseDFD
            )
        $ \_ -> exitEdhProc exit nil
    _ -> throwEdh EvalError "Invalid arg to `streamToDisk`"


-- | utility streamFromDisk(restoreOutlet, baseDFD)
streamFromDiskProc :: EdhProcedure
streamFromDiskProc (ArgsPack !args !kwargs) !exit = do
  pgs <- ask
  contEdhSTM $ do
    let procCtx   = edh'context pgs
        procScope = contextScope procCtx
        db        = thatObject procScope
    -- `that` object is the db instance, replace this host proc's top frame
    -- with a scope having the db instace available from attr `db`, at the
    -- same time lexcically nested within the host proc's original scope, so
    -- can read all attrs there.
    entWithDb <- createHashEntity
      $ Map.fromList [(AttrByName "db", EdhObject db)]
    let scopeWithDb = procScope
          { scopeEntity = entWithDb
          , scopeProc = (scopeProc procScope) { procedure'lexi = Just procScope
                                              }
          }
        !ctxWithDb =
          procCtx { callStack = scopeWithDb :| NE.tail (callStack procCtx) }
    case args of
      [EdhSink !restoreOutlet, EdhDecimal baseDFD] | Map.null kwargs ->
        -- not to use `unsafeIOToSTM` here, despite it being retry prone,
        -- nested `atomically` is particularly prohibited.
        edhPerformIO
            pgs
            ( streamEdhReprFromDisk ctxWithDb restoreOutlet
            $ fromIntegral
            $ D.castDecimalToInteger baseDFD
            )
          $ \_ -> exitEdhProc exit nil
      _ -> throwEdhSTM pgs EvalError "Invalid arg to `streamFromDisk`"

