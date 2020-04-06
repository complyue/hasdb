
module DB.Comm.Peer where

import           Prelude
-- import           Debug.Trace

import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Control.Concurrent.STM

import           Data.Hashable
import           Data.Text                      ( Text )
import qualified Data.Text                     as T

import           Language.Edh.EHI


type CmdDir = Text
type CmdSrc = Text
data CommCmd = CommCmd !CmdDir !CmdSrc
  deriving (Eq, Show)
instance Hashable CommCmd where
  hashWithSalt s (CommCmd cmd val) = s `hashWithSalt` cmd `hashWithSalt` val


data Peer = Peer {
      peer'ident :: !Text
    , peer'eos :: !(TMVar (Either SomeException ()))
    , peer'hosting :: !(TMVar CommCmd)
    , peer'posting :: !(TMVar CommCmd)
  } deriving (Eq)

readPeerCommand :: Peer -> EdhProcExit -> EdhProc
readPeerCommand (Peer !ident !eos !ho _) !exit = ask >>= \pgs ->
  contEdhSTM
    $ edhPerformIO
        pgs
        (atomically $ (Right <$> takeTMVar ho) `orElse` (Left <$> readTMVar eos)
        )
    $ \case
        Left (Right ()) -> exitEdhSTM pgs exit nil
        Left (Left ex) -> edhErrorFrom pgs ex $ \exv -> edhThrowSTM pgs exv
        Right (CommCmd !dir !src) -> case dir of
          "" ->
            runEdhProc pgs
              $ evalEdh (T.unpack ident) src
              $ \(OriginalValue !cmdVal _ _) -> exitEdhProc exit cmdVal
          "err" -> do
            let !ex = toException $ EdhPeerError ident src
            void $ tryPutTMVar eos $ Left ex
            edhErrorFrom pgs ex $ \exv -> edhThrowSTM pgs exv
          _ ->
            createEdhError pgs UsageError ("invalid packet directive: " <> dir)
              $ \exv ex -> do
                  void $ tryPutTMVar eos $ Left $ toException ex
                  edhThrowSTM pgs exv


