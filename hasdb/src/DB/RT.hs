
module DB.RT where

import           Prelude
-- import           Debug.Trace

import           System.IO
import           System.IO.Error
import           System.FilePath
import           System.Directory
import           System.Posix.IO
import           System.Posix.Types

import           Control.Exception
import           Control.Monad.Reader
import           Control.Concurrent.STM

import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.HashMap.Strict           as Map
import qualified Data.ByteString               as B
import           Data.Text
import qualified Data.Text                     as T
import           Data.Text.Encoding

import           Language.Edh.EHI


-- | utility className(*args,**kwargs)
classNameProc :: EdhProcedure
classNameProc !argsSender !exit = do
  !pgs <- ask
  let callerCtx   = edh'context pgs
      callerScope = contextScope callerCtx
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) ->
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
  classNameOf (EdhClass (ProcDefi _ (ProcDecl _ !cn _ _))) = EdhString cn
  classNameOf (EdhObject !obj) =
    EdhString $ procedure'name $ procedure'decl $ objClass obj
  classNameOf _ = nil


-- | utility newBo(boClass, sbEnt)
newBoProc :: EdhProcedure
newBoProc !argsSender !exit =
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhClass !cls, EdhObject !sbEnt] | Map.null kwargs ->
      createEdhObject cls $ \(OriginalValue boVal _ _) -> case boVal of
        EdhObject !bo -> do
          pgs <- ask
          let world = contextWorld $ edh'context pgs
          contEdhSTM $ do
            boScope <- mkScopeWrapper world $ objectScope bo
            modifyTVar' (objSupers bo) (sbEnt :)
            modifyTVar' (entity'store $ objEntity sbEnt)
              $ Map.insert (AttrByName "_boScope")
              $ EdhObject boScope
            exitEdhSTM pgs exit $ EdhObject bo
        _ -> error "bug: createEdhObject returned non-object"
    _ -> throwEdh EvalError "Invalid arg to `newBo`"


-- | utility backupToFile(persistOutlet, dataFileName)
--
-- this should be called from the main Edh thread, block it here until
-- db shutdown, or other Edh threads will be terminated, including
-- the one running the db app.
backupToFileProc :: EdhProcedure
backupToFileProc !argsSender !exit =
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhSink !persistOutlet, EdhString !dataFileName] | Map.null kwargs ->
      -- not to use `unsafeIOToSTM` here, despite it being retry prone,
      -- nested `atomically` is prohibited as well.
      edhWaitIO exit $ do
        backupEdhReprStreamToFile persistOutlet $ T.unpack dataFileName
        return nil
    _ -> throwEdh EvalError "Invalid arg to `backupToFile`"

-- | utility restoreFromFile(persistOutlet, dataFileName)
restoreFromFileProc :: EdhProcedure
restoreFromFileProc !argsSender !exit = undefined


restoreEdhReprStreamFromFile :: EventSink -> FilePath -> IO ()
restoreEdhReprStreamFromFile !sink !dataFilePath =
  withBinaryFile dataFilePath ReadMode $ \fileHndl -> do
    let pumpEvd = do
          atomically $ publishEvent sink nil
          -- loop again
          pumpEvd
    pumpEvd


backupEdhReprStreamToFile :: EventSink -> FilePath -> IO ()
backupEdhReprStreamToFile !sink !dataFilePath = do
  (subChan, _)    <- atomically $ subscribeEvents sink
  !dfBaselineTime <- dataFileTimeStamp
  bracket
      (openBackingFile
        (dataDir </> "wip-" <> dataFile <> "~" <> dfBaselineTime)
        1
      )
      (closeFd . snd)
    $ \(wipPath, wipFd) -> do
        fileHndl <- fdToHandle wipFd
        hSetBinaryMode fileHndl True
        let
          pumpEvd = atomically (readTChan subChan) >>= \case
            EdhNil -> return ()
            -- TODO support SCN (System-Change-Number) or other means of checkpointing
            --      schema, e.g. periodically inserted delimiter timestamp. then the
            --      replay/restore can have such checkpoint based range selection.
            !evd   -> do
              let !payload =
                    encodeUtf8 $ finishLine $ onSepLine $ edhValueStr evd
                  !pktLen = B.length payload
              -- write packet header
              B.hPut fileHndl $ encodeUtf8 $ T.pack $ "[" <> show pktLen <> "#]"
              -- write packet payload
              B.hPut fileHndl payload
              -- loop again
              pumpEvd
        pumpEvd
        void $ handleToFd fileHndl -- need its side-effect of flushing the write buf
        !dfObsoleteTimeStamp <- dataFileTimeStamp
        obsoleteBackingFile
          (   dataDir
          </> "old-"
          <>  dataFile
          <>  "~"
          <>  dfObsoleteTimeStamp
          <>  "-"
          <>  dfBaselineTime
          )
          1
        renameFile wipPath dataFilePath
 where
  (dataDir, dataFile) = splitFileName dataFilePath
  openBackingFile :: FilePath -> Int -> IO (FilePath, Fd)
  openBackingFile !baseName !seqN = do
    let !bfPath = baseName <> "." <> show seqN
    catchIOError
        ((bfPath, ) <$> openFd
          bfPath
          WriteOnly
          (Just 0o640)
          defaultFileFlags { nonBlock = True, exclusive = True }
        )
      $ \ioe -> if isAlreadyExistsError ioe
          then openBackingFile baseName (seqN + 1)
          else throwIO ioe
  obsoleteBackingFile :: FilePath -> Int -> IO ()
  obsoleteBackingFile !baseName !seqN = do
    let !bfPath = baseName <> "." <> show seqN
    catchIOError (renameFile dataFilePath bfPath) $ \ioe ->
      if isDoesNotExistError ioe
        then return () -- it's okay for absence of the file
        else if isAlreadyExistsError ioe
          then obsoleteBackingFile baseName (seqN + 1)
          else throwIO ioe
  onSepLine :: Text -> Text
  onSepLine "" = ""
  onSepLine !t = if "\n" `isPrefixOf` t then t else "\n" <> t
  finishLine :: Text -> Text
  finishLine "" = ""
  finishLine !t = if "\n" `isSuffixOf` t then t else t <> "\n"


dataFileTimeStamp :: IO String
dataFileTimeStamp =
  formatTime defaultTimeLocale "%Y%m%dT%H%M%S" <$> getCurrentTime


