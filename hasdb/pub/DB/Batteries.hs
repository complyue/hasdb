
module DB.Batteries
  ( installDbBatteries
  , module DB.Array
  )
where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader

import qualified Data.HashMap.Strict           as Map

import           DB.RT
import           DB.Array
import           DB.Storage.InMem

import           Language.Edh.EHI


installDbBatteries :: EdhWorld -> IO ()
installDbBatteries !world = do

  -- this host module only used by 'db/machinery'
  void $ installEdhModule world "db/Storage/InMem" $ \pgs exit -> do

    let moduScope = contextScope $ edh'context pgs
        modu      = thisObject moduScope

    !moduArts <-
      sequence
        $ [ (nm, ) <$> mkHostClass moduScope nm True hc
          | (nm, hc) <-
            [ ("BoSet"  , bosHostCtor)
            , ("BoIndex", boiHostCtor)
            , ("BuIndex", buiHostCtor)
            ]
          ]

    artsDict <- createEdhDict
      $ Map.fromList [ (EdhString k, v) | (k, v) <- moduArts ]
    updateEntityAttrs pgs (objEntity modu)
      $  [ (AttrByName k, v) | (k, v) <- moduArts ]
      ++ [(AttrByName "__exports__", artsDict)]


    exit

  -- this host module contains procedures to do persisting to and
  -- restoring from disk, the supporting classes / procedures for
  -- restoration must all be available from this module
  void $ installEdhModule world "db/RT" $ \pgs exit -> do

    let moduScope = contextScope $ edh'context pgs
        modu      = thisObject moduScope

    !moduArts <-
      sequence
      $  [ (nm, ) <$> mkHostProc moduScope mc nm hp args
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
      ++ [ (nm, ) <$> mkHostClass moduScope nm True hc
         | (nm, hc) <- [("DbArray", aryHostCtor)]
         ]

    artsDict <- createEdhDict
      $ Map.fromList [ (EdhString k, v) | (k, v) <- moduArts ]
    updateEntityAttrs pgs (objEntity modu)
      $  [ (AttrByName k, v) | (k, v) <- moduArts ]
      ++ [(AttrByName "__exports__", artsDict)]

    exit

