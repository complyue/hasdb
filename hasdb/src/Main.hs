
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

    void $ installEdhModule world "db/ehi" $ \modu -> do
      dbArts <- mapM
        (\(nm, hp) -> (AttrByName nm, ) <$> mkHostProc EdhHostProc nm hp)
        [ -- exload/imload only work for top level Edh procedures by far
          ("imload", imloadProc)
        , ("exload", exloadProc)
        ]

      installEdhAttrs (objEntity modu) dbArts

    modu <- createEdhModule world "<interactive>"
    doLoop world modu

