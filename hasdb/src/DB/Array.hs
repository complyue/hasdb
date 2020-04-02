{-# LANGUAGE GADTs #-}

module DB.Array where

import           Prelude
-- import           Debug.Trace

import           System.IO.MMap

import           Foreign

import           Control.Monad.Reader
import           Control.Concurrent.STM

import           Data.List.NonEmpty             ( NonEmpty(..) )
import qualified Data.List.NonEmpty            as NE

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import           Data.Dynamic

import           Data.Vector                    ( Vector )
import qualified Data.Vector                   as V
import qualified Data.Vector.Storable          as I
import qualified Data.Vector.Storable.Mutable  as M
import qualified Data.Vector.Generic           as G

import qualified Data.Lossless.Decimal         as D
import           Language.Edh.EHI


-- | Named dimensions with size of each
type ArrayShape = NonEmpty (Text, Int)

-- Dense Array wrapped as Edh object
data DbArray a where
 DbArray ::Storable a => {
      dbArrayPath :: !FilePath    -- ^ data file path relative to data root dir
    , dbArrayShape :: !ArrayShape -- ^ shape of dimensions
    , dbArrayLen1D :: !Int        -- ^ valid length of 1st dimension
    , dbArrayData :: !(ForeignPtr a) -- ^ mmap'ed data pointer
  } -> DbArray a

dbArraySize :: DbArray a -> Int
dbArraySize !ary = product (snd <$> dbArrayShape ary)


type DbF8Array = DbArray Double
type DbF4Array = DbArray Float


-- | host constructor Array(dataDir, dataPath, shape, dtype, initial=0)
aryHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store 
  -> STM ()
aryHostCtor !pgsCtor (ArgsPack !ctorArgs !ctorKwargs) !obs = do
  let !scope = contextScope $ edh'context pgsCtor
      !this  = thisObject scope
  let doIt :: Int -> [EdhValue] -> STM ()
      doIt !len !vs = do
        let ary = case len of
              _ | len < 0 -> V.fromList vs
              _           -> V.fromListN len vs
        writeTVar (entity'store $ objEntity this) $ toDyn ary
        methods <- sequence
          [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
          | (nm, vc, hp, mthArgs) <-
            [ ( "[]"
              , EdhMethod
              , aryIdxReadProc
              , PackReceiver [RecvArg "idx" Nothing Nothing]
              )
            , ("__repr__", EdhMethod, aryReprProc, PackReceiver [])
            , ("all"     , EdhMethod, aryAllProc , PackReceiver [])
            ]
          ]
        modifyTVar' obs
          $  Map.union
          $  Map.fromList
          $  methods
          ++ [(AttrByName "length", EdhDecimal $ fromIntegral $ V.length ary)]
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

  aryIdxReadProc :: EdhProcedure
  aryIdxReadProc (ArgsPack !args !_kwargs) !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing ->
          throwEdhSTM pgs UsageError $ "bug: this is not a ary : " <> T.pack
            (show esd)
        Just (ary :: DbF8Array) -> case args of
          [EdhDecimal !idx] -> case fromInteger <$> D.decimalToInteger idx of
            Just n ->
              let i = if n >= 0 then n else dbArraySize ary + n
              in  if i < 0 || i >= dbArraySize ary
                    then
                      throwEdhSTM pgs EvalError
                      $  "Vector index out of bounds: "
                      <> T.pack (show idx)
                      <> " against length "
                      <> T.pack (show $ dbArraySize ary)
                    else exitEdhSTM pgs exit nil
            _ ->
              throwEdhSTM pgs UsageError
                $  "Not an integer for index: "
                <> T.pack (show idx)
          -- TODO support slicing
          _ -> throwEdhSTM pgs UsageError "Invalid index for a Vector"

  aryReprProc :: EdhProcedure
  aryReprProc _ !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing ->
          throwEdhSTM pgs UsageError $ "bug: this is not a ary : " <> T.pack
            (show esd)
        Just (ary :: DbF8Array) ->
          exitEdhSTM pgs exit $ EdhString "db.Array(xxx, dtype='f8')"

  aryAllProc :: EdhProcedure
  aryAllProc _ !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing ->
          throwEdhSTM pgs UsageError $ "bug: this is not a ary : " <> T.pack
            (show esd)
        Just (ary :: DbF8Array) -> exitEdhSTM pgs exit nil

