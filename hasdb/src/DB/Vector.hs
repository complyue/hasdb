
module DB.Vector where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader
import           Control.Concurrent.STM

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import           Data.Dynamic

import           Data.Vector                    ( Vector )
import qualified Data.Vector                   as V

import qualified Data.Lossless.Decimal         as D

import           Language.Edh.EHI


-- Boxed Vector for Edh values
type EdhVector = Vector EdhValue

-- | host constructor Vector(*elements,length=None)
vecHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store 
  -> (Dynamic -> STM ())  -- in-band data to be written to entity store
  -> STM ()
vecHostCtor !pgsCtor (ArgsPack !ctorArgs !ctorKwargs) !obs !ctorExit = do
  let !scope = contextScope $ edh'context pgsCtor
      doIt :: Int -> [EdhValue] -> STM ()
      doIt !len !vs = do
        let vec = case len of
              _ | len < 0 -> V.fromList vs
              _           -> V.fromListN len vs
        methods <- sequence
          [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
          | (nm, vc, hp, mthArgs) <-
            [ ( "[]"
              , EdhMethod
              , vecIdxReadProc
              , PackReceiver [RecvArg "idx" Nothing Nothing]
              )
            , ("__repr__", EdhMethod, vecReprProc, PackReceiver [])
            , ("all"     , EdhMethod, vecAllProc , PackReceiver [])
            ]
          ]
        modifyTVar' obs
          $  Map.union
          $  Map.fromList
          $  methods
          ++ [ (AttrByName "__null__", EdhBool $ V.length vec <= 0)
             , (AttrByName "length"  , EdhDecimal $ fromIntegral $ V.length vec)
             ]
        ctorExit $ toDyn vec
  case Map.lookup "length" ctorKwargs of
    Nothing              -> doIt (-1) ctorArgs
    Just (EdhDecimal !d) -> case D.decimalToInteger d of
      Just len | len >= 0 -> doIt (fromInteger len) $ ctorArgs ++ repeat nil
      _ ->
        throwEdhSTM pgsCtor UsageError $ "Length not an integer: " <> T.pack
          (show d)
    Just !badLenVal ->
      throwEdhSTM pgsCtor UsageError $ "Invalid length: " <> T.pack
        (show badLenVal)

 where

  vecIdxReadProc :: EdhProcedure
  vecIdxReadProc (ArgsPack !args !_kwargs) !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing ->
          throwEdhSTM pgs UsageError $ "bug: this is not a vec : " <> T.pack
            (show esd)
        Just (vec :: EdhVector) -> case args of
          [EdhDecimal !idx] -> case fromInteger <$> D.decimalToInteger idx of
            Just n ->
              let i = if n < 0
                    then -- python style negative indexing
                         V.length vec + n
                    else n
              in  if i < 0 || i >= V.length vec
                    then
                      throwEdhSTM pgs EvalError
                      $  "Vector index out of bounds: "
                      <> T.pack (show idx)
                      <> " against length "
                      <> T.pack (show $ V.length vec)
                    else exitEdhSTM pgs exit $ V.unsafeIndex vec i
            _ ->
              throwEdhSTM pgs UsageError
                $  "Not an integer for index: "
                <> T.pack (show idx)
          -- TODO support slicing
          _ -> throwEdhSTM pgs UsageError "Invalid index for a Vector"

  vecReprProc :: EdhProcedure
  vecReprProc _ !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
        go :: [EdhValue] -> [Text] -> STM ()
        go [] !rs =
          exitEdhSTM pgs exit
            $  EdhString
            $  "Vector("
            <> T.intercalate "," (reverse rs)
            <> ")"
        go (v : rest) rs =
          runEdhProc pgs $ edhValueRepr v $ \(OriginalValue !rv _ _) ->
            case rv of
              EdhString !r -> contEdhSTM $ go rest (r : rs)
              _            -> error "bug: edhValueRepr returned non-string"
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing ->
          throwEdhSTM pgs UsageError $ "bug: this is not a vec : " <> T.pack
            (show esd)
        Just (vec :: EdhVector) -> go (V.toList vec) []

  vecAllProc :: EdhProcedure
  vecAllProc _ !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing ->
          throwEdhSTM pgs UsageError $ "bug: this is not a vec : " <> T.pack
            (show esd)
        Just (vec :: EdhVector) ->
          exitEdhSTM pgs exit $ EdhTuple $ V.toList vec
