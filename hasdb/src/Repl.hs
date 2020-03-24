
module Repl where

import           Prelude
-- import           Debug.Trace

import           Text.Printf

import           Control.Monad.Reader

import           Control.Concurrent.STM

import           Data.Text                      ( Text )
import qualified Data.Text                     as T

import           System.Console.Haskeline

import           Language.Edh.EHI

import           DB.RT


-- | Manage lifecycle of Edh programs during the repl session
edhProgLoop :: TQueue EdhConsoleIO -> EdhWorld -> IO ()
edhProgLoop !ioQ !world = do

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

  loop
 where
  loop = do
    runEdhModule world "db" >>= \case
      Left  err -> atomically $ writeTQueue ioQ $ ConsoleOut $ T.pack $ show err
      Right ()  -> pure ()
    atomically $ writeTQueue ioQ $ ConsoleOut "🐴🐴🐯🐯"
    loop


-- | Serialize output to `stdout` from Edh programs, and give them command
-- line input when requested
ioLoop :: TQueue EdhConsoleIO -> InputT IO ()
ioLoop !ioQ = liftIO (atomically $ readTQueue ioQ) >>= \case
  ConsoleShutdown    -> return () -- gracefully stop the io loop
  ConsoleOut !txtOut -> do
    outputStrLn $ T.unpack txtOut
    ioLoop ioQ
  ConsoleIn !cmdIn !ps1 !ps2 -> readInput ps1 ps2 [] >>= \case
    Nothing -> -- reached EOF (end-of-feed)
      return ()
    Just !cmd -> do -- got one piece of code
      liftIO $ atomically $ putTMVar cmdIn cmd
      ioLoop ioQ


-- | The repl line reader
readInput :: Text -> Text -> [Text] -> InputT IO (Maybe Text)
readInput !ps1 !ps2 !initialLines =
  handleInterrupt ( -- start over on Ctrl^C
                   outputStrLn "" >> readLines [])
    $ withInterrupt
    $ readLines initialLines
 where
  readLines pendingLines = getInputLine prompt >>= \case
    Nothing -> case pendingLines of
      [] -> return Nothing
      _ -> -- TODO warn about premature EOF ?
        return Nothing
    Just text ->
      let code = T.pack text
      in  case pendingLines of
            [] -> case T.stripEnd code of
              "{" -> -- an unindented `{` marks start of multi-line input
                readLines [""]
              _ -> case T.strip code of
                "" -> -- got an empty line in single-line input mode
                  readLines [] -- start over in single-line input mode
                _ -> -- got a single line input
                  return $ Just code
            _ -> case T.stripEnd code of
              "}" -> -- an unindented `}` marks end of multi-line input
                return $ Just $ (T.unlines . reverse) $ init pendingLines
              _ -> -- got a line in multi-line input mode
                readLines $ code : pendingLines
   where
    prompt :: String
    prompt = case pendingLines of
      [] -> T.unpack ps1
      _  -> T.unpack ps2 <> printf "%2d" (length pendingLines) <> ": "
