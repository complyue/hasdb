
module Repl where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader

import           Control.Concurrent.STM

import qualified Data.Text                     as T

import           Language.Edh.EHI

import           DB.RT


-- | Manage lifecycle of Edh programs during the repl session
edhProgLoop :: EdhRuntime -> IO ()
edhProgLoop !runtime = do

  -- create the world, we always work with this world no matter how
  -- many times the Edh programs crash
  world <- createEdhWorld runtime
  installEdhBatteries world

  -- install the host module
  void $ installEdhModule world "db/ehi" $ \pgs modu -> do

    let ctx       = edh'context pgs
        moduScope = objectScope ctx modu

    !dbArts <-
      sequence
      $  [ (AttrByName nm, ) <$> mkHostProc moduScope mc nm hp args
         | (mc, nm, hp, args) <-
           [ (EdhMethod, "className", classNameProc, WildReceiver)
           , ( EdhMethod
             , "newBo"
             , newBoProc
             , PackReceiver
               [ RecvArg "boClass" Nothing Nothing
               , RecvArg "sbEnt"   Nothing Nothing
               ]
             )
           , ( EdhMethod
             , "streamToDisk"
             , streamToDiskProc
             , PackReceiver
               [ RecvArg "persistOutlet"  Nothing Nothing
               , RecvArg "dataFileFolder" Nothing Nothing
               , RecvArg "sinkBaseDFD"    Nothing Nothing
               ]
             )
           , ( EdhMethod
             , "streamFromDisk"
             , streamFromDiskProc
             , PackReceiver
               [ RecvArg "restoreOutlet" Nothing Nothing
               , RecvArg "baseDFD"       Nothing Nothing
               ]
             )
           ]
         ]
      ++ [ (AttrByName nm, ) <$> mkHostClass moduScope nm True hc
         | (nm, hc) <- [("BoIndex", boiHostCtor), ("BoSet", bosHostCtor)]
         ]

    updateEntityAttrs pgs (objEntity modu) dbArts

  -- to run a module is to seek its `__main__.edh` and execute the
  -- code there in a volatile module context, it can import itself
  -- (i.e. `__init__.edh`) during the run. the imported module can
  -- survive program crashes as all imported modules do.
  let loop = runEdhModule world "db" >>= \case
        Left err -> do -- program crash on error
          atomically $ writeTQueue ioQ $ ConsoleOut $ T.pack $ show err
          -- the world with all modules ever imported, are still there,
          atomically $ writeTQueue ioQ $ ConsoleOut "🐴🐴🐯🐯"
          -- repeat another repl session with this world.
          -- it may not be a good idea, but just so so ...
          loop
        Right phv -> case edhUltimate phv of
          EdhNil -> do -- clean program halt, all done
            atomically $ writeTQueue ioQ $ ConsoleOut
              "Your work committed, bye."
            atomically $ writeTQueue ioQ ConsoleShutdown
          _ -> do -- unclean program exit
            atomically $ writeTQueue ioQ $ ConsoleOut
              "Your program halted with a result:"
            atomically $ writeTQueue ioQ $ ConsoleOut $ case phv of
              EdhString msg -> msg
              _             -> T.pack $ show phv
            -- the world with all modules ever imported, are still there,
            atomically $ writeTQueue ioQ $ ConsoleOut "🐴🐴🐯🐯"
            -- repeat another repl session with this world.
            -- it may not be a good idea, but just so so ...
            loop
  loop
  where ioQ = consoleIO runtime
