
module DB.EHI
  ( installDbBatteries
  , module DB.Array
  )
where

import           Prelude
-- import           Debug.Trace

import           Control.Monad.Reader

import           DB.RT
import           DB.Array
import           DB.Storage.InMem

import           Language.Edh.EHI

import           Dim.EHI


installDbBatteries :: EdhWorld -> IO ()
installDbBatteries !world = do

  installDimBatteries world


  -- this host module only used by 'db/machinery'
  void $ installEdhModule world "db/Storage/InMem" $ \pgs exit -> do

    declareEdhOperators world
                        "<hasdb>"
                        [
          -- throw a business object away from a business collection
                         ("^*", 1)]

    let moduScope = contextScope $ edh'context pgs
        modu      = thisObject moduScope

    !moduArts <- sequence
      [ ((nm, ) <$>) $ mkExtHostClass moduScope nm hc $ \ !classUniq ->
          createSideEntityManipulater True =<< mths classUniq pgs
      | (nm, hc, mths) <-
        [ ("BoSet"  , bosCtor, bosMethods)
        , ("BoIndex", boiCtor, boiMethods)
        , ("BuIndex", buiCtor, buiMethods)
        ]
      ]

    artsDict <- createEdhDict [ (EdhString k, v) | (k, v) <- moduArts ]
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
           [ ( EdhMethod
             , "newBo"
             , newBoProc
             , PackReceiver [mandatoryArg "boClass", mandatoryArg "sbEnt"]
             )
           , ( EdhMethod
             , "streamToDisk"
             , streamToDiskProc
             , PackReceiver
               [ mandatoryArg "persistOutlet"
               , mandatoryArg "dataFileFolder"
               , mandatoryArg "sinkBaseDFD"
               ]
             )
           , ( EdhMethod
             , "streamFromDisk"
             , streamFromDiskProc
             , PackReceiver
               [mandatoryArg "restoreOutlet", mandatoryArg "baseDFD"]
             )
           ]
         ]
      ++ [ ((nm, ) <$>) $ mkExtHostClass moduScope nm hc $ \ !classUniq ->
             createSideEntityManipulater True =<< mths classUniq pgs
         | (nm, hc, mths) <- [("DbArray", aryCtor, aryMethods)]
         ]

    artsDict <- createEdhDict [ (EdhString k, v) | (k, v) <- moduArts ]
    updateEntityAttrs pgs (objEntity modu)
      $  [ (AttrByName k, v) | (k, v) <- moduArts ]
      ++ [(AttrByName "__exports__", artsDict)]

    exit

