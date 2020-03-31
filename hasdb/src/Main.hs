
module Main where

import           Prelude
-- import           Debug.Trace

import           Control.Monad
import           Control.Exception
import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.Text                     as T

import           Language.Edh.EHI

import           Repl

main :: IO ()
main = do

  console <- defaultEdhConsole defaultEdhConsoleSettings
  let consoleOut = writeTQueue (consoleIO console) . ConsoleOut

  void $ forkFinally (edhProgLoop console) $ \result -> do
    case result of
      Left (e :: SomeException) ->
        atomically $ consoleOut $ "💥 " <> T.pack (show e)
      Right _ -> pure ()
    -- shutdown console IO anyway
    atomically $ writeTQueue (consoleIO console) ConsoleShutdown

  atomically $ do
    consoleOut ">> Haskell Data Back <<\n"
    consoleOut
      "* Blank Screen Syndrome ? Take the Tour as your companion, checkout:\n"
    consoleOut "  https://github.com/e-wrks/hasdb/tree/master/Tour\n"

  consoleIOLoop console
