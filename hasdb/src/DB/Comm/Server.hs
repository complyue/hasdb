
module DB.Comm.Server where

import           Prelude
import           Debug.Trace

import           System.IO

import           Control.Exception
import           Control.Applicative
import           Control.Monad
import           Control.Monad.Reader
import           Control.Concurrent
import           Control.Concurrent.STM

import           Data.Text                      ( Text )
import qualified Data.Text                     as T
import           Data.Text.Encoding

import           Network.Socket

import           Language.Edh.EHI

import           DB.Comm.MicroProto


servEdhClients :: Context -> Text -> Int -> IO ()
servEdhClients !ctx !servAddr !servPort = withSocketsDo $ do
  addr <- resolveServAddr
  bracket (open addr) close acceptClients
 where
  resolveServAddr = do
    let hints =
          defaultHints { addrFlags = [AI_PASSIVE], addrSocketType = Stream }
    addr : _ <- getAddrInfo (Just hints)
                            (Just $ T.unpack servAddr)
                            (Just (show servPort))
    return addr
  open addr = do
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    setSocketOption sock ReuseAddr 1
    bind sock (addrAddress addr)
    listen sock 30
    return sock
  acceptClients :: Socket -> IO ()
  acceptClients sock = do
    (conn, _) <- accept sock
    clientId  <- show <$> getPeerName conn
    hndl      <- socketToHandle conn ReadWriteMode
    void $ forkFinally (servClient clientId hndl) $ \result -> do
      case result of
        Left  exc -> trace ("Edh Client error: " <> show exc) $ pure ()
        Right _   -> pure ()
      hClose hndl -- close the socket anyway
    acceptClients sock -- tail recursion

  servClient :: String -> Handle -> IO ()
  servClient !clientId !hndl = do
    thClient              <- myThreadId
    (pktSink, stopSignal) <- atomically $ liftA2 (,) newEmptyTMVar newEmptyTMVar
    let
      servPump :: EdhProgState -> STM ()
      servPump !pgs =
        edhPerformIO
            pgs
            (        atomically
            $        (   Right
                     <$> (takeTMVar pktSink >>= \(dir, payload) ->
                           return (dir, decodeUtf8 payload)
                         )
                     )
            `orElse` (Left <$> readTMVar stopSignal)
            )
          $ \case
          -- client closed connection
          -- TODO some cleanup here
              Left  _             -> return ()
          -- parse & eval
              Right (dir, edhSrc) -> case dir of
                "" -> runEdhProc pgs $ evalEdh clientId edhSrc $ \_ ->
                  contEdhSTM $ servPump pgs -- CPS recursion
                _ ->
                  throwEdhSTM pgs UsageError
                    $  "invalid packet directive: "
                    <> dir
    void
      -- start another Edh program to serve this client
      $ forkFinally
          (runEdhProgram' ctx (ask >>= \pgs -> contEdhSTM $ servPump pgs))
      $ \result -> do
          case result of
            -- to cancel socket reading as well as propagate the error
            Left  e -> throwTo thClient e
            Right _ -> pure ()
          -- TODO cleanup client
          return ()
    -- pump incoming src commands
    parsePackets hndl pktSink stopSignal

