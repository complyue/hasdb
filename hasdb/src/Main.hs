
module Main
  ( main
  )
where

import           Prelude
-- import           Debug.Trace

import           Control.Monad

import           System.Console.Haskeline

import           Language.Edh.EHI

import           DB.RT

import           Repl                           ( doLoop )


inputSettings :: Settings IO
inputSettings = Settings { complete       = \(_left, _right) -> return ("", [])
                         , historyFile    = Nothing
                         , autoAddHistory = True
                         }


main :: IO ()
main = do

  -- todo create a logger coop'ing with haskeline specifically ?
  logger <- defaultEdhLogger

  runInputT inputSettings $ do

    outputStrLn ">> Haskell Data Back <<"

    world <- createEdhWorld logger
    installEdhBatteries world

    -- install the host module
    void $ installEdhModule world "db/ehi" $ \modu -> do

      let moduScope = objectScope modu

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
        ++ [ (AttrByName nm, ) <$> mkHostClass moduScope nm hc
           | (nm, hc) <- [("BoIndex", boiHostCtor)]
           ]

      updateEntityAttrs (objEntity modu) dbArts

    modu <- createEdhModule world "<interactive>" "<adhoc>"
    doLoop world modu

