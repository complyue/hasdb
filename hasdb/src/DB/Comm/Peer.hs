
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
  contEdhSTM $ tryReadTMVar eos >>= \case
    Just (Right ()) -> exitEdhSTM pgs exit EdhNil
    Just (Left  e ) -> edhErrorFrom pgs e $ \exv -> edhThrowSTM pgs exv
    Nothing         -> takeTMVar ho >>= \(CommCmd !dir !src) -> case dir of
      "err" -> do
        let !ex = toException $ EdhPeerError ident src
        void $ tryPutTMVar eos $ Left ex
        edhErrorFrom pgs ex $ \exv -> edhThrowSTM pgs exv
      "" ->
        runEdhProc pgs
          $ evalEdh (T.unpack ident) src
          $ \(OriginalValue !cmdVal _ _) -> exitEdhProc exit cmdVal
      _ -> throwEdhSTM pgs UsageError $ "invalid packet directive: " <> dir


