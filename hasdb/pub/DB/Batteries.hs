
module DB.Batteries
  ( installDbBatteries
  , EdhVector
  )
where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader

import           DB.RT
import           DB.Vector
import           DB.Storage.InMem

import           Language.Edh.EHI


installDbBatteries :: EdhWorld -> IO ()
installDbBatteries !world = do

  -- this host module only used by 'db/machinery'
  void $ installEdhModule world "db/Storage/InMem" $ \pgs modu -> do

    let ctx       = edh'context pgs
        moduScope = objectScope ctx modu

    !moduArts <-
      sequence
        $ [ (AttrByName nm, ) <$> mkHostClass moduScope nm True hc
          | (nm, hc) <-
            [ ("BoSet"  , bosHostCtor)
            , ("BoIndex", boiHostCtor)
            , ("BuIndex", buiHostCtor)
            ]
          ]

    updateEntityAttrs pgs (objEntity modu) moduArts

  -- this host module contains procedures to do persisting to and
  -- restoring from disk, the supporting classes / procedures for
  -- restoration must all be available from this module
  void $ installEdhModule world "db/RT" $ \pgs modu -> do

    let ctx       = edh'context pgs
        moduScope = objectScope ctx modu

    !moduArts <-
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
         | (nm, hc) <- [("Vector", vecHostCtor)]
         ]

    updateEntityAttrs pgs (objEntity modu) moduArts

