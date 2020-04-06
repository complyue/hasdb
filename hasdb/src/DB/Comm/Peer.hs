
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
    , peer'posting :: !(TQueue CommCmd)
  } deriving (Eq)

readPeerCommand :: Peer -> EdhProcExit -> EdhProc
readPeerCommand (Peer !ident !eos !ho po) !exit = ask >>= \pgs ->
  contEdhSTM
    $ edhPerformIO
        pgs
        (atomically $ (Right <$> takeTMVar ho) `orElse` (Left <$> readTMVar eos)
        )
    $ \case
        Left (Right ()) -> exitEdhSTM pgs exit nil
        Left (Left ex) -> toEdhError pgs ex $ \exv -> edhThrowSTM pgs exv
        Right (CommCmd !dir !src) -> case dir of
          "" ->
            runEdhProc pgs
              $ edhCatch (evalEdh (T.unpack ident) src) exit
              $ \_recover rethrow -> do
                  pgsPassOn <- ask
                  let !exv = contextMatch $ edh'context pgsPassOn
                  if exv == nil -- no exception occurred,
                    then rethrow -- rethrow just passes on in this case
                    else contEdhSTM $ edhValueReprSTM pgs exv $ \exr -> do
                      -- send peer the error details
                      writeTQueue po $ CommCmd "err" exr
                      -- mark eos with this error
                      fromEdhError pgs exv
                        $ \e -> void $ tryPutTMVar eos $ Left e
                      -- rethrow the error
                      runEdhProc pgs rethrow
          "err" -> do
            let !ex = toException $ EdhPeerError ident src
            void $ tryPutTMVar eos $ Left ex
            toEdhError pgs ex $ \exv -> edhThrowSTM pgs exv
          _ ->
            createEdhError pgs UsageError ("invalid packet directive: " <> dir)
              $ \exv ex -> do
                  void $ tryPutTMVar eos $ Left $ toException ex
                  edhThrowSTM pgs exv


