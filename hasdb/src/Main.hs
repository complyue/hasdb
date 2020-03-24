
module Main
  ( main
  )
where

import           Prelude
-- import           Debug.Trace

import           Control.Monad
import           Control.Exception
import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.Text                     as T

import           System.Console.Haskeline       ( runInputT
                                                , Settings(..)
                                                , outputStrLn
                                                )

import           Language.Edh.EHI

import           Repl


inputSettings :: Settings IO
inputSettings = Settings { complete       = \(_left, _right) -> return ("", [])
                         , historyFile    = Nothing
                         , autoAddHistory = True
                         }


main :: IO ()
main = do

  ioQ     <- newTQueueIO
  runtime <- defaultEdhRuntime ioQ

  void $ forkFinally (edhProgLoop runtime) $ \result -> do
    case result of
      Left (e :: SomeException) ->
        atomically $ writeTQueue ioQ $ ConsoleOut $ "💥 " <> T.pack (show e)
      Right _ -> atomically $ writeTQueue ioQ $ ConsoleOut "Bye."
    atomically $ writeTQueue ioQ ConsoleShutdown

  runInputT inputSettings $ do
    outputStrLn ">> Haskell Data Back <<"
    ioLoop ioQ

  flushRuntimeLogs runtime
