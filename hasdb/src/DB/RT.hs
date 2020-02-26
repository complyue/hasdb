
module DB.RT where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader
import           Control.Concurrent.STM

import           Language.Edh.EHI


-- | utility imload(*args,**kwargs)
imloadProc :: EdhProcedure
imloadProc !argsSender !exit =
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> if null kwargs
    then case args of
      []  -> exitEdhProc exit nil
      [v] -> imloadValue v exit
      _   -> imloadValues [] args exit
    else throwEdh EvalError "not impl. yet"

imloadValues :: [EdhValue] -> [EdhValue] -> EdhProcExit -> EdhProg (STM ())
imloadValues !loaded [] !exit = exitEdhProc exit $ EdhTuple $ reverse loaded
imloadValues !loaded (v : rest) !exit = imloadValue v
  $ \(OriginalValue !lv _ _) -> imloadValues (lv : loaded) rest exit

imloadValue :: EdhValue -> EdhProcExit -> EdhProg (STM ())
imloadValue !val !exit = case val of
  EdhPair (EdhString moduName) (EdhString attrName) ->
    importEdhModule moduName $ \(OriginalValue moduVal _ _) -> case moduVal of
      EdhObject !modu -> do
        pgs <- ask
        contEdhSTM $ do
          moduScope <- moduleScope (contextWorld $ edh'context pgs) modu
          lookupEdhCtxAttr moduScope (AttrByName attrName) >>= \case
            Nothing       -> exitEdhSTM pgs exit nil
            Just !attrVal -> exitEdhSTM pgs exit attrVal
      _ -> error "bug: importEdhModule gives non-object"
  _ ->
    throwEdh EvalError
      $  "don't know how to imload a "
      <> edhValueStr (edhTypeOf val)
      <> ": "
      <> edhValueStr val


-- | utility exload(*args,**kwargs)
exloadProc :: EdhProcedure
exloadProc !argsSender !exit = do
  !pgs <- ask
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> contEdhSTM $ do
    argsResult <- sequence $ (fmap $ exloadValue pgs) args
    if null kwargs
      then case argsResult of
        []  -> exitEdhSTM pgs exit nil
        [v] -> exitEdhSTM pgs exit v
        _   -> exitEdhSTM pgs exit (EdhTuple argsResult)
      else do
        kwargsResult <- sequence $ (fmap $ exloadValue pgs) kwargs
        exitEdhSTM pgs exit $ EdhArgsPack $ ArgsPack argsResult kwargsResult

exloadValue :: EdhProgState -> EdhValue -> STM EdhValue
exloadValue !pgs !val = case val of
  EdhClass  !defi       -> exloadProcDefi defi
  EdhMethod !defi       -> exloadProcDefi defi
  EdhOperator _ _ !defi -> exloadProcDefi defi
  EdhGenrDef     !defi  -> exloadProcDefi defi
  EdhInterpreter !defi  -> exloadProcDefi defi
  _ ->
    throwEdhSTM pgs EvalError
      $  "don't know how to exload a "
      <> edhValueStr (edhTypeOf val)
      <> ": "
      <> edhValueStr val

-- | this blindly assumes a procedure is defined at top level, can not be
-- right unless used carefully.
exloadProcDefi :: ProcDefi -> STM EdhValue
exloadProcDefi !proc = case procedure'lexi proc of
  Nothing     -> return nil
  Just !scope -> lookupEdhCtxAttr scope (AttrByName "__name__") >>= \case
    Nothing -> return nil
    Just moduName ->
      return $ EdhPair moduName $ EdhString $ procedure'name $ procedure'decl
        proc

