
module DB.RT where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader

import qualified Data.HashMap.Strict           as Map

import           Language.Edh.EHI


-- | utility className(*args,**kwargs)
classNameProc :: EdhProcedure
classNameProc !argsSender !exit = do
  !pgs <- ask
  let callerCtx   = edh'context pgs
      callerScope = contextScope callerCtx
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) ->
    let !argsCls = classNameOf <$> args
    in  if null kwargs
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
  classNameOf (EdhClass (ProcDefi _ (ProcDecl _ cn _ _))) = EdhString cn
  classNameOf _ = nil


-- | utility newBo(*args,**kwargs)
newBoProc :: EdhProcedure
newBoProc !argsSender !exit =
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhClass !cls] | Map.null kwargs ->
      createEdhObject cls $ \(OriginalValue boVal _ _) -> case boVal of
        EdhObject !bo -> do
          pgs <- ask
          let world = contextWorld $ edh'context pgs
          contEdhSTM $ do
            boScope <- mkScopeWrapper world $ objectScope bo
            exitEdhSTM pgs exit $ EdhTuple [EdhObject bo, EdhObject boScope]
        _ -> error "bug: createEdhObject returned non-object"
    _ -> throwEdh EvalError "should pass the class as the only arg to `newBo`"

