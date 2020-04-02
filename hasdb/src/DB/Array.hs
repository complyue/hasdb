{-# LANGUAGE GADTs #-}

module DB.Array where

import           Prelude
-- import           Debug.Trace

import           GHC.Conc                       ( unsafeIOToSTM )

import           System.FilePath
import           System.Directory
import           System.IO.MMap

import           Foreign

import           Control.Monad.Primitive
import           Control.Monad.Reader
import           Control.Concurrent.STM

import           Data.List
import           Data.List.NonEmpty             ( NonEmpty(..) )
import qualified Data.List.NonEmpty            as NE

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import           Data.Dynamic

import qualified Data.Vector.Storable          as I
import qualified Data.Vector.Storable.Mutable  as M

import qualified Data.Lossless.Decimal         as D
import           Language.Edh.EHI


-- | The shape of an array is named dimensions with size of each
-- zero-dimension shape is prohibited here (by NonEmpty list as container)
-- zero-sized dimensions are prohibited by parseArrayShape
newtype ArrayShape = ArrayShape (NonEmpty (DimName, DimSize)) deriving (Eq, Typeable)
instance Show ArrayShape where
  show (ArrayShape shape) =
    "("
      <> intercalate
           ", "
           (   (\(n, s) ->
                 if n == "" then show s else T.unpack n <> " := " <> show s
               )
           <$> NE.toList shape
           )
      <> ")"
type DimName = Text
type DimSize = Int
parseArrayShape :: EdhProgState -> EdhValue -> (ArrayShape -> STM ()) -> STM ()
parseArrayShape !pgs !val !exit = case val of
  EdhTuple (dim1 : dims) -> parseDim dim1 $ \pd ->
    seqcontSTM (parseDim <$> dims) $ \pds -> exit $ ArrayShape $ pd :| pds
  _ -> edhValueReprSTM pgs val
    $ \r -> throwEdhSTM pgs UsageError $ "Invalid array shape spec: " <> r
 where
  parseDim :: EdhValue -> ((DimName, DimSize) -> STM ()) -> STM ()
  parseDim v@(EdhDecimal d) !exit' = case D.decimalToInteger d of
    Just size | size > 0 -> exit' ("", fromInteger size)
    _                    -> edhValueReprSTM pgs v
      $ \r -> throwEdhSTM pgs UsageError $ "Invalid dimension size: " <> r
  parseDim v@(EdhNamedValue name (EdhDecimal d)) !exit' =
    case D.decimalToInteger d of
      Just size | size > 0 -> exit' (name, fromInteger size)
      _                    -> edhValueReprSTM pgs v
        $ \r -> throwEdhSTM pgs UsageError $ "Invalid dimension size: " <> r
  parseDim v _ = edhValueReprSTM pgs v
    $ \r -> throwEdhSTM pgs UsageError $ "Invalid dimension spec: " <> r
edhArrayShape :: ArrayShape -> EdhValue
edhArrayShape (ArrayShape !shape) = EdhTuple $! edhDim <$> NE.toList shape
 where
  edhDim :: (DimName, DimSize) -> EdhValue
  edhDim (""  , size) = EdhDecimal $ fromIntegral size
  edhDim (name, size) = EdhNamedValue name $ EdhDecimal $ fromIntegral size

dbArraySize :: ArrayShape -> Int
dbArraySize (ArrayShape !shape) = product (snd <$> shape)

-- TODO make error msg more friendly
flatIndexInShape
  :: EdhProgState -> [EdhValue] -> ArrayShape -> (Int -> STM ()) -> STM ()
flatIndexInShape !pgs !idxs (ArrayShape !shape) !exit =
  if length idxs /= NE.length shape
    then throwEdhSTM pgs UsageError $ "NDim of index mismatch shape"
    else flatIdx (reverse idxs) (reverse $ NE.toList shape) exit
 where
  flatIdx :: [EdhValue] -> [(DimName, DimSize)] -> (Int -> STM ()) -> STM ()
  flatIdx [] [] !exit' = exit' 0
  flatIdx (EdhDecimal d : restIdxs) ((_, ds) : restDims) !exit' =
    case D.decimalToInteger d of
      Just d' ->
        let s' = case fromIntegral d' of
              s | s < 0 -> s + ds  -- numpy style negative indexing
              s         -> s
        in  if s' < 0 || s' >= ds
              then
                throwEdhSTM pgs UsageError
                $  "Index out of bounds: "
                <> T.pack (show d')
                <> " vs "
                <> T.pack (show ds)
              else flatIdx restIdxs restDims $ \i -> exit' $ ds * i + s'
      Nothing ->
        throwEdhSTM pgs UsageError $ "Index not an integer: " <> T.pack (show d)
  flatIdx _ _ _ = throwEdhSTM pgs UsageError "Invalid index"


-- | The meta data about an array
data ArrayMeta = ArrayMeta {
      dbArrayPath :: !Text        -- ^ data file path relative to data root dir
    , dbArrayShape :: !ArrayShape -- ^ shape of dimensions
    , dbArrayLen1d :: !Int        -- ^ valid length of 1st dimension
  } deriving (Eq, Show, Typeable)


data DbArray e where {
    DbArray ::(Storable e, Num e) => ArrayMeta -> (ForeignPtr e) -> DbArray e
  } deriving (Typeable)

arrayMeta :: (Storable e, Num e) => DbArray e -> ArrayMeta
arrayMeta (DbArray !meta _) = meta

arrayFlatVector :: (Storable e, Num e) => DbArray e -> I.Vector e
arrayFlatVector (DbArray (ArrayMeta _ !shape _) !fptr) =
  I.unsafeFromForeignPtr fptr 0 (dbArraySize shape)

arrayFlatMVector
  :: (Storable e, Num e) => DbArray e -> (M.MVector (PrimState IO) e)
arrayFlatMVector (DbArray (ArrayMeta _ !shape _) !fptr) =
  M.unsafeFromForeignPtr fptr 0 (dbArraySize shape)

mmapArray
  :: forall e . (Storable e, Num e) => Text -> ArrayMeta -> IO (DbArray e)
mmapArray !dataDir meta@(ArrayMeta dataPath shape _) = do
  let !dataFilePath = T.unpack dataDir </> T.unpack (dataPath <> ".edf")
      !dataFileDir  = takeDirectory dataFilePath
  createDirectoryIfMissing True dataFileDir
  (fptr, _, _) <- mmapFileForeignPtr dataFilePath ReadWriteEx
    $ Just (0, dbArraySize shape * sizeOf (undefined :: e))
  return $ DbArray meta fptr


type DbF8Array = DbArray Double
type DbF4Array = DbArray Float


-- | host constructor Array(dataDir, dataPath, shape, dtype='f8', len1d=0)
aryHostCtor
  :: EdhProgState
  -> ArgsPack  -- ctor args, if __init__() is provided, will go there too
  -> TVar (Map.HashMap AttrKey EdhValue)  -- out-of-band attr store
  -> (Dynamic -> STM ())  -- in-band data to be written to entity store
  -> STM ()
aryHostCtor !pgsCtor !apk !obs !ctorExit =
  case parseArgsPack ("", "", nil, "f8", 0 :: Int) ctorArgsParser apk of
    Left err -> throwEdhSTM pgsCtor UsageError err
    Right (dataDir, dataPath, shapeVal, dtype, len1d) ->
      if dataDir == "" || dataPath == ""
        then throwEdhSTM pgsCtor UsageError "Missing dataDir/dataPath"
        else parseArrayShape pgsCtor shapeVal $ \shape -> do
          let
            !scope = contextScope $ edh'context pgsCtor
            doIt !dary = do
              methods <- sequence
                [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
                | (nm, vc, hp, mthArgs) <-
                  [ ( "[]"
                    , EdhMethod
                    , aryIdxReadProc
                    , PackReceiver [RecvArg "idx" Nothing Nothing]
                    )
                  , ("__repr__", EdhMethod, aryReprProc, PackReceiver [])
                  , ("all"     , EdhGnrtor, aryAllProc , PackReceiver [])
                  ]
                ]
              modifyTVar' obs
                $  Map.union
                $  Map.fromList
                $  methods
                ++ [ (AttrByName "path" , EdhString dataPath)
                   , (AttrByName "shape", edhArrayShape shape)
                   , (AttrByName "dtype", EdhString dtype)
                   , (AttrByName "len1d", EdhDecimal $ fromIntegral len1d)
                   , ( AttrByName "size"
                     , EdhDecimal $ fromIntegral $ dbArraySize shape
                     )
                   ]
              ctorExit dary
          case dtype of
            "f8" -> do
              ary :: DbF8Array <- unsafeIOToSTM $ mmapArray dataDir $ ArrayMeta
                dataPath
                shape
                len1d
              doIt $ toDyn ary
            "f4" -> do
              ary :: DbF4Array <- unsafeIOToSTM $ mmapArray dataDir $ ArrayMeta
                dataPath
                shape
                len1d
              doIt $ toDyn ary
            _ ->
              throwEdhSTM pgsCtor UsageError $ "Unsupported dtype: " <> dtype
 where
  ctorArgsParser =
    ArgsPackParser
        [ \arg (_, dataPath', shape', dtype', len1d') -> case arg of
          EdhString dataDir ->
            Right (dataDir, dataPath', shape', dtype', len1d')
          _ -> Left "Invalid dataDir"
        , \arg (dataDir', _, shape', dtype', len1d') -> case arg of
          EdhString dataPath ->
            Right (dataDir', dataPath, shape', dtype', len1d')
          _ -> Left "Invalid dataPath"
        , \arg (dataDir', dataPath', _, dtype', len1d') ->
          Right (dataDir', dataPath', arg, dtype', len1d')
        ]
      $ Map.fromList
          [ ( "dtype"
            , \arg (dataDir', dataPath', shape', _, len1d') -> case arg of
              EdhString dtype ->
                Right (dataDir', dataPath', shape', dtype, len1d')
              _ -> Left "Invalid dtype object"
            )
          , ( "len1d"
            , \arg (dataDir', dataPath', shape', dtype', _) -> case arg of
              EdhDecimal !d -> case D.decimalToInteger d of
                Just len1d | len1d >= 0 ->
                  Right (dataDir', dataPath', shape', dtype', fromInteger len1d)
                _ -> Left "Not an positive integer for len1d"
              _ -> Left "Invalid len1d"
            )
          ]

  aryIdxReadProc :: EdhProcedure
  aryIdxReadProc (ArgsPack !args _) !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing -> case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs UsageError $ "bug: this is not an ary : " <> T.pack
              (show esd)
          Just (_ary :: DbF4Array) ->
            throwEdhSTM pgs UsageError "dtype=f4 not impl. yet"
        Just (ary :: DbF8Array) -> case args of
          -- TODO support slicing, of coz need to tell a slicing index from
          --      an element index first
          [EdhTuple !vs] ->
            flatIndexInShape pgs vs (dbArrayShape $ arrayMeta ary)
              $ \flatIdx -> do
                  let flatVec = arrayFlatVector ary
                      ev      = I.unsafeIndex flatVec flatIdx
                  exitEdhSTM pgs exit $ EdhDecimal $ fromRational $ realToFrac
                    ev
          idx ->
            flatIndexInShape pgs idx (dbArrayShape $ arrayMeta ary)
              $ \flatIdx -> do
                  let flatVec = arrayFlatVector ary
                      ev      = I.unsafeIndex flatVec flatIdx
                  exitEdhSTM pgs exit $ EdhDecimal $ fromRational $ realToFrac
                    ev

  aryReprProc :: EdhProcedure
  aryReprProc _ !exit = do
    pgs <- ask
    let this = thisObject $ contextScope $ edh'context pgs
        es   = entity'store $ objEntity this
    contEdhSTM $ do
      esd <- readTVar es
      case fromDynamic esd of
        Nothing -> case fromDynamic esd of
          Nothing ->
            throwEdhSTM pgs UsageError $ "bug: this is not an ary : " <> T.pack
              (show esd)
          Just ((DbArray (ArrayMeta !path !shape !len1d) _) :: DbF4Array) ->
            exitEdhSTM pgs exit
              $  EdhString
              $  "db.Array("
              <> T.pack (show path)
              <> ", "
              <> T.pack (show shape)
              <> ", dtype='f4', len1d="
              <> T.pack (show len1d)
              <> ")"
        Just ((DbArray (ArrayMeta !path !shape !len1d) _) :: DbF8Array) ->
          exitEdhSTM pgs exit
            $  EdhString
            $  "db.Array("
            <> T.pack (show path)
            <> ", "
            <> T.pack (show shape)
            <> ", dtype='f8', len1d="
            <> T.pack (show len1d)
            <> ")"

  aryAllProc :: EdhProcedure
  aryAllProc _ !exit = do
    pgs <- ask
    case generatorCaller $ edh'context pgs of
      Nothing -> throwEdh UsageError "Can only be called as generator"
      Just (!pgs', !iter'cb) -> contEdhSTM $ do
        let this = thisObject $ contextScope $ edh'context pgs
            !es  = entity'store $ objEntity this
        esd <- readTVar es
        case fromDynamic esd of
          Nothing -> case fromDynamic esd of
            Nothing ->
              throwEdhSTM pgs UsageError
                $  "bug: this is not an ary : "
                <> T.pack (show esd)
            Just (_ary :: DbF4Array) ->
              throwEdhSTM pgs UsageError "dtype=f4 not impl. yet"
          Just (ary :: DbF8Array) -> do
            let flatVec = arrayFlatVector ary
                flatLen = I.length flatVec
                -- TODO yield ND index instead of flat index
                yieldResults :: Int -> STM ()
                yieldResults !i = if i >= flatLen
                  then exitEdhSTM pgs exit nil
                  else
                    let ev = I.unsafeIndex flatVec i
                    in  runEdhProc pgs'
                        $ iter'cb
                            (EdhArgsPack
                              (ArgsPack
                                [ EdhDecimal $ fromIntegral i
                                , EdhDecimal $ fromRational $ realToFrac ev
                                ]
                                mempty
                              )
                            )
                        $ \_ -> yieldResults (i + 1)
            yieldResults 0

