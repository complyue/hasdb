
module DB.Storage.DataDir where

import           Prelude
-- import           Debug.Trace

import           System.IO
import           System.IO.Error
import           System.FilePath
import           System.Directory
import           System.Posix
import qualified System.Posix.Files.ByteString as PB

import           Control.Monad
import           Control.Monad.Reader
import           Control.Exception
import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.Text                     as T
import           Data.Text.Encoding
import           Data.Time.Clock
import           Data.Time.Format

import qualified Data.ByteString               as B
import qualified Data.ByteString.Char8         as C
import           Data.Text

import           Language.Edh.EHI
import           Language.Edh.Net


edhDataFileToken, latestEdhDataFileName :: String
edhDataFileToken = "edf"
latestEdhDataFileName = "latest." <> edhDataFileToken

dataFileTimeStamp :: IO String
dataFileTimeStamp =
  formatTime defaultTimeLocale "%Y%m%dT%H%M%S" <$> getCurrentTime


streamEdhReprFromDisk :: Context -> EventSink -> Fd -> IO ()
streamEdhReprFromDisk !ctx !restoreOutlet !dfd = if dfd < 0
  then -- okay for the latest data file to be absent
       -- mark eos of the outlet and done
       atomically $ publishEvent restoreOutlet nil
  else restoreLatest
 where
  restoreLatest = bracket (fdToHandle dfd) handleToFd $ \fileHndl -> do
    -- seems that closing an Fd will fail hard after `fdToHandle` but before
    -- `handleToFd`. may be an undocumented side-effect ?
    hSetBinaryMode fileHndl True
    pktSink <- newEmptyTMVarIO
    eos     <- newEmptyTMVarIO
    let
      restorePump :: EdhProgState -> STM ()
      restorePump !pgs =
        edhPerformIO
            pgs
            (        atomically
            $        (   Right
                     <$> (takeTMVar pktSink >>= \(dir, payload) ->
                           return (dir, decodeUtf8 payload)
                         )
                     )
            `orElse` (Left <$> readTMVar eos)
            )
          $ \case
          -- stopped, mark eos of the outlet and done
              Left  _          -> publishEvent restoreOutlet nil
          -- parse & eval evs to Edh value, then post to restoreOutlet 
              Right (dir, evs) -> case dir of
                "" ->
                  runEdhProc pgs
                    $ evalEdh "<edf>" evs
                    $ \(OriginalValue evd _ _) -> contEdhSTM $ do
                        publishEvent restoreOutlet evd
                        restorePump pgs -- CPS recursion
                _ ->
                  throwEdhSTM pgs UsageError
                    $  "invalid packet directive: "
                    <> dir

    -- pump out file contents from another dedicated thread
    void $ forkIO $ receivePacketStream ("FD#" <> T.pack (show dfd))
                                        fileHndl
                                        pktSink
                                        eos

    -- run another Edh program to parse & eval disk file content
    try (runEdhProgram' ctx (ask >>= \pgs -> contEdhSTM $ restorePump pgs))
      >>= \result -> do
            case result of
              -- try mark eos for the file anyway
              Left  e -> void $ atomically $ tryPutTMVar eos $ Left e
              Right _ -> void $ atomically $ tryPutTMVar eos $ Right ()
            -- mark eos of the outlet anyway
            atomically $ publishEvent restoreOutlet nil


streamEdhReprToDisk :: Context -> EventSink -> FilePath -> EventSink -> IO ()
streamEdhReprToDisk !ctx !persitOutlet !dataFileFolder !sinkBaseDFD =
  bracket openBaseFile (\(_, dfd) -> unless (dfd < 0) $ closeFd dfd)
    $ \(baseDFN, !baseDFD) -> do
        dbLaunchTimeStamp <- dataFileTimeStamp
        let baseDFV =
              if B.null baseDFN then "epoch" else T.unpack $ decodeUtf8 dfv
            (_, dfv) = C.breakEnd (== '-') baseDFN
        atomically $ publishEvent sinkBaseDFD $ EdhDecimal $ fromIntegral
          baseDFD
        bracket
            (openWorkFile
              (  edhDataFileToken
              <> "-wip-"
              <> baseDFV
              <> "-"
              <> dbLaunchTimeStamp
              )
              1
            )
            (closeFd . snd)
          $ \(wipPath, wipFd) ->
              -- seems that closing an Fd will fail hard after `fdToHandle` but before
              -- `handleToFd`. may be an undocumented side-effect ?
              -- and `handleToFd` is especially needed here for its documented side-effect
              -- of flushing the write buf.
              bracket (fdToHandle wipFd) handleToFd $ \fileHndl -> do
                txtVar <- newTVarIO (Nothing :: Maybe Text)
                hSetBinaryMode fileHndl True
                (subChan, _) <- atomically $ subscribeEvents persitOutlet
                let
                  pumpEvd = do
                    void $ runEdhProgram' ctx $ do
                      pgs <- ask
                      contEdhSTM $ waitEdhSTM pgs (readTChan subChan) $ \case
                        EdhNil -> -- end-of-stream reached
                          writeTVar txtVar Nothing
  -- TODO support SCN (System-Change-Number) or other means of checkpointing
  --      schema, e.g. periodically inserted delimiter timestamp. then the
  --      replay/restore can have such checkpoint based range selection.
                        !evd ->
                          runEdhProc pgs
                            $ edhValueRepr evd
                            $ \(OriginalValue evr _ _) -> case evr of
                                EdhString evrs ->
                                  contEdhSTM $ writeTVar txtVar $ Just evrs
                                _ ->
                                  error
                                    "bug: edhValueRepr returned non-string in CPS"
                    readTVarIO txtVar >>= \case
                      Nothing   -> return () -- eos 
                      Just !txt -> do
                        sendTextPacket (T.pack wipPath) fileHndl "" txt
                        -- keep pumping
                        pumpEvd
                pumpEvd
                !dbShutdownTimeStamp <- dataFileTimeStamp
                pubName              <- commitDataFile
                  wipPath
                  (  edhDataFileToken
                  <> "-"
                  <> baseDFV
                  <> "-"
                  <> dbShutdownTimeStamp
                  )
                  1
                latestDFN <- readLatestDFN
                if latestDFN /= baseDFN
                  then -- other data file commited before this db run, leave this session's
                       -- result data file there without marking it as the latest.
                       return ()
                  else do
                    let !pubPath = dataFileFolder </> latestEdhDataFileName
                    -- rm the latest pointer file, or creating a new one will fail for sure
                    catchIOError (removeFile pubPath)
                      $ \ioe -> if isDoesNotExistError ioe
                          then return ()
                          else throwIO ioe
                    -- still a race condition here, but it can be seemingly no better
                    createSymbolicLink pubName pubPath

 where

  readLatestDFN :: IO B.ByteString
  readLatestDFN =
    catchIOError
        (   PB.readSymbolicLink
        $   encodeUtf8
        $   T.pack
        $   dataFileFolder
        </> latestEdhDataFileName
        )
      $ \ioe -> if isDoesNotExistError ioe then return "" else throwIO ioe

  openBaseFile :: IO (B.ByteString, Fd)
  openBaseFile = readLatestDFN >>= \case
    ""        -> return ("", Fd (-1))
    latestDFN -> (latestDFN, ) <$> openFd
      (dataFileFolder </> T.unpack (decodeUtf8 latestDFN))
      ReadOnly
      Nothing
      defaultFileFlags { nonBlock = True }

  openWorkFile :: FilePath -> Int -> IO (FilePath, Fd)
  openWorkFile !baseName !seqN = do
    let !wipPath = dataFileFolder </> baseName <> "." <> show seqN
    catchIOError
        ((wipPath, ) <$> openFd
          wipPath
          WriteOnly
          (Just 0o640)
          defaultFileFlags { nonBlock = True, exclusive = True }
        )
      $ \ioe -> if isAlreadyExistsError ioe
          then openWorkFile baseName (seqN + 1)
          else throwIO ioe

  commitDataFile :: FilePath -> FilePath -> Int -> IO FilePath
  commitDataFile !wipPath !baseName !seqN = do
    let !pubName = baseName <> "." <> show seqN
        !pubPath = dataFileFolder </> pubName
    catchIOError (pubName <$ renameFile wipPath pubPath) $ \ioe ->
      if isDoesNotExistError ioe
        then return "" -- wip file lost, manually renamed or unlinked
        else if isAlreadyExistsError ioe
          then commitDataFile wipPath baseName (seqN + 1)
          else throwIO ioe
