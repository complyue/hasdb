
module DB.RT where

import           Prelude
-- import           Debug.Trace

-- import           System.IO.Unsafe

import           Control.Monad.Reader
-- import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.HashMap.Strict           as Map
import qualified Data.Text                     as T

import           Data.Lossless.Decimal          ( castDecimalToInteger )
import           Language.Edh.EHI

import           DB.Storage.DataDir


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


-- | utility newBo(boClass, sbEnt)
newBoProc :: EdhProcedure
newBoProc !argsSender !exit =
  packEdhArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhClass !cls, EdhObject !sbEnt] | Map.null kwargs ->
      createEdhObject cls [] $ \(OriginalValue boVal _ _) -> case boVal of
        EdhObject !bo -> do
          pgs <- ask
          let world = contextWorld $ edh'context pgs
          contEdhSTM $ do
            boScope <- mkScopeWrapper world $ objectScope bo
            modifyTVar' (objSupers bo) (sbEnt :)
            modifyTVar' (entity'store $ objEntity sbEnt) $ \es ->
              changeEntityAttr es (AttrByName "_boScope") $ EdhObject boScope
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

