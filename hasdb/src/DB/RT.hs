
module DB.RT where

import           Prelude
-- import           Debug.Trace

-- import           GHC.Conc                       ( unsafeIOToSTM )

import           Control.Monad.Reader
-- import           Control.Concurrent
import           Control.Concurrent.STM

import           Data.List.NonEmpty             ( NonEmpty(..) )
import qualified Data.List.NonEmpty            as NE
import qualified Data.Text                     as T
-- import           Data.Unique
-- import           Data.Dynamic


import qualified Data.Lossless.Decimal         as D
import           Language.Edh.EHI

import           DB.Storage.DataDir


-- | utility newBo(boClass, sbObj)
-- this performs non-standard business object construction
newBoProc :: EdhProcedure
newBoProc (ArgsPack !args !kwargs) !exit = case args of
  [EdhClass !cls, EdhObject !sbObj] | odNull kwargs ->
    createEdhObject cls (ArgsPack [] odEmpty) $ \(OriginalValue boVal _ _) ->
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
                        $ callEdhMethod bo mth'proc (ArgsPack [] odEmpty) id
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
streamToDiskProc (ArgsPack [EdhSink !persistOutlet, EdhString !dataFileFolder, EdhSink !sinkBaseDFD] !kwargs) !exit
  | odNull kwargs
  = ask >>= \pgs ->
    contEdhSTM
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
streamToDiskProc _ _ = throwEdh EvalError "Invalid arg to `streamToDisk`"


-- | utility streamFromDisk(restoreOutlet, baseDFD)
streamFromDiskProc :: EdhProcedure
streamFromDiskProc (ArgsPack !args !kwargs) !exit = do
  pgs <- ask
  contEdhSTM $ do
-- run in parent context with artifacts necessary for persitent repr restoration
    let procCtx     = edh'context pgs
        parentScope = contextFrame procCtx 1
    let !parentCtx =
          procCtx { callStack = parentScope :| NE.tail (callStack procCtx) }
    case args of
      [EdhSink !restoreOutlet, EdhDecimal baseDFD] | odNull kwargs ->
        -- not to use `unsafeIOToSTM` here, despite it being retry prone,
        -- nested `atomically` is particularly prohibited.
        edhPerformIO
            pgs
            ( streamEdhReprFromDisk parentCtx restoreOutlet
            $ fromIntegral
            $ D.castDecimalToInteger baseDFD
            )
          $ \_ -> exitEdhProc exit nil
      _ -> throwEdhSTM pgs EvalError "Invalid arg to `streamFromDisk`"

