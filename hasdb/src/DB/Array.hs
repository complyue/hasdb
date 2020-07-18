
module DB.Array where

import           Prelude
-- import           Debug.Trace

import           GHC.Conc                       ( unsafeIOToSTM )

import           System.FilePath
import           System.Directory
import           System.IO.MMap

import           Foreign

import           Control.Concurrent.STM

import           Data.List.NonEmpty             ( NonEmpty(..) )
import qualified Data.List.NonEmpty            as NE

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import qualified Data.HashMap.Strict           as Map
import           Data.Unique
import           Data.Coerce
import           Data.Dynamic

import qualified Data.Lossless.Decimal         as D
import           Language.Edh.EHI

import           Dim.EHI


-- | The shape of an array is named dimensions with size of each
-- zero-dimension shape is prohibited here (by NonEmpty list as container)
-- zero-sized dimensions are prohibited by parseArrayShape
newtype ArrayShape = ArrayShape (NonEmpty (DimName, DimSize)) deriving (Eq, Typeable)
instance Show ArrayShape where
  show (ArrayShape shape) =
    concat
      $  ["("]
      ++ (   (\(n, s) ->
               (if n == "" then show s else T.unpack n <> " := " <> show s) <> ", "
             )
         <$> NE.toList shape
         )
      ++ [")"]
type DimName = Text
type DimSize = Int
parseArrayShape :: EdhProgState -> EdhValue -> (ArrayShape -> STM ()) -> STM ()
parseArrayShape !pgs !val !exit = case val of
  EdhArgsPack (ArgsPack (dim1 : dims) _) -> parseDim dim1 $ \pd ->
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
edhArrayShape (ArrayShape !shape) = EdhArgsPack
  $ ArgsPack (edhDim <$> NE.toList shape) odEmpty
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


data DbArray where
  DbArray ::(Storable a, EdhXchg a) => {
      db'array'path  :: !Text           -- ^ data file path relative to root
    , db'array'shape :: !ArrayShape     -- ^ shape of dimensions
    , db'array'dti   :: !DataTypeIdent  -- ^ dtype identifier
    , db'array'store :: !(FlatArray a)  -- ^ flat storage
    , db'array'len1d :: !(TVar Int)     -- ^ valid length of 1st dimension
    } -> DbArray
  deriving (Typeable)

-- | unwrap an array from Edh object form
unwrapArrayObject :: Object -> STM (Maybe DbArray)
unwrapArrayObject !ao = fromDynamic <$> readTVar (entity'store $ objEntity ao)


mmapArray :: Text -> Text -> ArrayShape -> DataType -> Int -> STM DbArray
mmapArray !dataDir !dataPath !shape (DataType !dti (dts :: FlatStorable a)) !len1d
  = do
    len1dVar <- newTVar len1d
    unsafeIOToSTM $ do
  -- XXX this is retry prone, has to be solid reliable when rapidly retried
  --     with the result possibly discarded
  -- TODO battle test this impl.
      let !dataFilePath = T.unpack dataDir </> T.unpack (dataPath <> ".edf")
          !dataFileDir  = takeDirectory dataFilePath
          !cap          = dbArraySize shape
      createDirectoryIfMissing True dataFileDir
      (fp, _, _) <- mmapFileForeignPtr dataFilePath ReadWriteEx
        $ Just (0, cap * flat'element'size dts)
      return $ DbArray @a dataPath shape dti (FlatArray cap fp) len1dVar


-- | host constructor DbArray(dataDir, dataPath, shape, dtype='float64', len1d=0)
aryCtor :: EdhHostCtor
aryCtor !pgsCtor !apk !ctorExit =
  case parseArgsPack ("", "", nil, "float64", 0 :: Int) ctorArgsParser apk of
    Left err -> throwEdhSTM pgsCtor UsageError err
    Right (dataDir, dataPath, shapeVal, dti, len1d) ->
      if dataDir == "" || dataPath == ""
        then throwEdhSTM pgsCtor UsageError "Missing dataDir/dataPath"
        else parseArrayShape pgsCtor shapeVal $ \ !shape ->
          resolveDataType pgsCtor dti $ \ !dt -> do
            ary <- mmapArray dataDir dataPath shape dt len1d
            ctorExit $ toDyn ary
 where
  ctorArgsParser =
    ArgsPackParser
        [ \arg (_, dataPath', shape', dti', len1d') -> case arg of
          EdhString dataDir -> Right (dataDir, dataPath', shape', dti', len1d')
          _                 -> Left "Invalid dataDir"
        , \arg (dataDir', _, shape', dti', len1d') -> case arg of
          EdhString dataPath ->
            Right (dataDir', dataPath, shape', dti', len1d')
          _ -> Left "Invalid dataPath"
        , \arg (dataDir', dataPath', _, dti', len1d') ->
          Right (dataDir', dataPath', arg, dti', len1d')
        , \arg (dataDir', dataPath', shape', _, len1d') ->
          case edhUltimate arg of
            EdhString !dti -> Right (dataDir', dataPath', shape', dti, len1d')
            !badDtype      -> Left $ "Bad dtype identifier of " <> T.pack
              (edhTypeNameOf badDtype)
        ]
      $ Map.fromList
          [ ( "dtype"
            , \arg (dataDir', dataPath', shape', _, len1d') ->
              case edhUltimate arg of
                EdhString !dti ->
                  Right (dataDir', dataPath', shape', dti, len1d')
                !badDtype -> Left $ "Bad dtype identifier of " <> T.pack
                  (edhTypeNameOf badDtype)
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

aryMethods :: Unique -> EdhProgState -> STM [(AttrKey, EdhValue)]
aryMethods !classUniq !pgsModule =
  sequence
    $  [ (AttrByName nm, ) <$> mkHostProc scope vc nm hp mthArgs
       | (nm, vc, hp, mthArgs) <-
         [ ("[]", EdhMethod, aryIdxReadProc, PackReceiver [mandatoryArg "idx"])
         , ( "[=]"
           , EdhMethod
           , aryIdxWriteProc
           , PackReceiver [mandatoryArg "idx", mandatoryArg "val"]
           )
         , ("__repr__", EdhMethod, aryReprProc, PackReceiver [])
         ]
       ]
    ++ [ (AttrByName nm, ) <$> mkHostProperty scope nm getter setter
       | (nm, getter, setter) <-
         [ ("path" , aryPathGetter , Nothing)
         , ("shape", aryShapeGetter, Nothing)
         , ("dtype", aryDtypeGetter, Nothing)
         , ("len1d", aryLen1dGetter, Nothing)
         , ("size" , arySizeGetter , Nothing)
         ]
       ]
 where
  !scope = contextScope $ edh'context pgsModule

  aryPathGetter :: EdhProcedure
  aryPathGetter _ !exit = withEntityOfClass classUniq
    $ \ !pgs !ary -> exitEdhSTM pgs exit $ EdhString $ db'array'path ary

  aryShapeGetter :: EdhProcedure
  aryShapeGetter _ !exit = withEntityOfClass classUniq
    $ \ !pgs !ary -> exitEdhSTM pgs exit $ edhArrayShape $ db'array'shape ary

  aryDtypeGetter :: EdhProcedure
  aryDtypeGetter _ !exit = withEntityOfClass classUniq
    $ \ !pgs !ary -> exitEdhSTM pgs exit $ EdhString $ db'array'dti ary

  aryLen1dGetter :: EdhProcedure
  aryLen1dGetter _ !exit = withEntityOfClass classUniq $ \ !pgs !ary ->
    readTVar (db'array'len1d ary)
      >>= \ !len1d -> exitEdhSTM pgs exit $ EdhDecimal $ fromIntegral len1d

  arySizeGetter :: EdhProcedure
  arySizeGetter _ !exit = withEntityOfClass classUniq $ \ !pgs !ary ->
    exitEdhSTM pgs exit
      $ EdhDecimal
      $ fromIntegral
      $ dbArraySize
      $ db'array'shape ary

  aryIdxReadProc :: EdhProcedure
  aryIdxReadProc (ArgsPack !args _) !exit =
    withEntityOfClass classUniq $ \ !pgs (DbArray _ !shape !dti !fa _l1dv) ->
      resolveDataType pgs dti $ \(DataType _dti !dts) -> case args of
          -- TODO support slicing, of coz need to tell a slicing index from
          --      an element index first
        [EdhArgsPack (ArgsPack !idxs _)] ->
          flatIndexInShape pgs idxs shape $ \ !flatIdx ->
            flat'array'read dts pgs (coerce fa) flatIdx
              $ \ !rv -> exitEdhSTM pgs exit rv
        idxs -> flatIndexInShape pgs idxs shape $ \ !flatIdx ->
          flat'array'read dts pgs (coerce fa) flatIdx
            $ \ !rv -> exitEdhSTM pgs exit rv


  aryIdxWriteProc :: EdhProcedure
  aryIdxWriteProc (ArgsPack !args _) !exit =
    withEntityOfClass classUniq $ \ !pgs (DbArray _ !shape !dti !fa _l1dv) ->
      resolveDataType pgs dti $ \(DataType _dti !dts) -> case args of
          -- TODO support slicing assign, of coz need to tell a slicing index
          --      from an element index first
        [EdhArgsPack (ArgsPack !idxs _), !dv] ->
          flatIndexInShape pgs idxs shape $ \ !flatIdx ->
            flat'array'write dts pgs (coerce fa) flatIdx dv
              $ \ !rv -> exitEdhSTM pgs exit rv
        [idx, !dv] -> flatIndexInShape pgs [idx] shape $ \ !flatIdx ->
          flat'array'write dts pgs (coerce fa) flatIdx dv
            $ \ !rv -> exitEdhSTM pgs exit rv
        -- TODO more friendly error msg
        _ -> throwEdhSTM pgs UsageError "Invalid index assign args"

  aryReprProc :: EdhProcedure
  aryReprProc _ !exit =
    withEntityOfClass classUniq
      $ \ !pgs (DbArray !path !shape !dti _fa !l1dv) ->
          readTVar l1dv >>= \ !len1d ->
            exitEdhSTM pgs exit
              $  EdhString
              $  "db.Array("
              <> T.pack (show path)
              <> ", "
              <> T.pack (show shape)
              <> ", dtype="
              <> dti
              <> ", len1d="
              <> T.pack (show len1d)
              <> ")"

