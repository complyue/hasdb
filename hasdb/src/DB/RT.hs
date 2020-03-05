
module DB.RT where

import           Prelude
-- import           Debug.Trace

import           GHC.Conc                       ( unsafeIOToSTM )

import           System.IO

import           Control.Monad.Reader
import           Control.Concurrent
import           Control.Concurrent.STM

import qualified Data.HashMap.Strict           as Map
import qualified Data.ByteString               as B
import           Data.Text
import qualified Data.Text                     as T
import           Data.Text.Encoding

import           Language.Edh.EHI


-- | utility className(*args,**kwargs)
classNameProc :: EdhProcedure
classNameProc !argsSender !exit = do
  !pgs <- ask
  let callerCtx   = edh'context pgs
      callerScope = contextScope callerCtx
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) ->
    let !argsCls = classNameOf <$> args
    in  if Map.null kwargs
          then case argsCls of
            [] ->
              exitEdhProc exit (EdhClass $ objClass $ thisObject callerScope)
            [t] -> exitEdhProc exit t
            _   -> exitEdhProc exit (EdhTuple argsCls)
          else exitEdhProc
            exit
            (EdhArgsPack $ ArgsPack argsCls $ Map.map classNameOf kwargs)
 where
  classNameOf :: EdhValue -> EdhValue
  classNameOf (EdhClass (ProcDefi _ (ProcDecl _ !cn _ _))) = EdhString cn
  classNameOf (EdhObject !obj) =
    EdhString $ procedure'name $ procedure'decl $ objClass obj
  classNameOf _ = nil


-- | utility newBo(boClass, sbEnt)
newBoProc :: EdhProcedure
newBoProc !argsSender !exit =
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhClass !cls, EdhObject !sbEnt] | Map.null kwargs ->
      createEdhObject cls $ \(OriginalValue boVal _ _) -> case boVal of
        EdhObject !bo -> do
          pgs <- ask
          let world = contextWorld $ edh'context pgs
          contEdhSTM $ do
            boScope <- mkScopeWrapper world $ objectScope bo
            modifyTVar' (objSupers bo) (sbEnt :)
            modifyTVar' (entity'store $ objEntity sbEnt)
              $ Map.insert (AttrByName "_boScope")
              $ EdhObject boScope
            exitEdhSTM pgs exit $ EdhObject bo
        _ -> error "bug: createEdhObject returned non-object"
    _ -> throwEdh EvalError "Invalid arg to `newBo`"


-- | utility backToFile(persistOutlet, dataFileName)
backToFileProc :: EdhProcedure
backToFileProc !argsSender !exit = do
  !pgs <- ask
  packHostProcArgs argsSender $ \(ArgsPack !args !kwargs) -> case args of
    [EdhSink !persistOutlet, EdhString dataFileName] | Map.null kwargs ->
      contEdhSTM $ do
        shutdownSig <- newEmptyTMVar
        unsafeIOToSTM
          -- can not `atomically` during `unsafeIOToSTM`, fork a thread to do that
          $ void
          $ forkIO
          $ do
              backEdhReprStreamToFile persistOutlet $ T.unpack dataFileName
              atomically $ putTMVar shutdownSig nil
        -- this should be called from the main Edh thread, block it here until
        -- db shutdown, or other Edh threads will be terminated, including
        -- the one running the db app.
        waitEdhSTM pgs (readTMVar shutdownSig) $ exitEdhSTM pgs exit
    _ -> throwEdh EvalError "Invalid arg to `backToFile`"

backEdhReprStreamToFile :: EventSink -> FilePath -> IO ()
backEdhReprStreamToFile !sink !filepath =
  withBinaryFile filepath WriteMode $ \fileHndl -> do
    (subChan, _) <- atomically $ subscribeEvents sink
    let
      pumpEvd = atomically (readTChan subChan) >>= \case
        EdhNil -> return ()
        !evd   -> do
          let !payload = encodeUtf8 $ finishLine $ onSepLine $ edhValueStr evd
              !pktLen  = B.length payload
          -- write packet header
          B.hPut fileHndl $ encodeUtf8 $ T.pack $ "[" <> show pktLen <> "#]"
          -- write packet payload
          B.hPut fileHndl payload
          -- loop again
          pumpEvd
    pumpEvd
 where
  onSepLine :: Text -> Text
  onSepLine "" = ""
  onSepLine !t = if "\n" `isPrefixOf` t then t else "\n" <> t
  finishLine :: Text -> Text
  finishLine "" = ""
  finishLine !t = if "\n" `isSuffixOf` t then t else t <> "\n"


