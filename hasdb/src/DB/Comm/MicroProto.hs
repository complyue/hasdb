
module DB.Comm.MicroProto where

import           Prelude
-- import           Debug.Trace

import           System.IO

import           Control.Exception
import           Control.Monad.Reader
import           Control.Concurrent.STM
import qualified Data.ByteString               as B
import qualified Data.ByteString.Char8         as C
import           Data.Text
import qualified Data.Text                     as T
import           Data.Text.Encoding


maxHeaderLength :: Int
maxHeaderLength = 60

type PacketDirective = Text
type PacketPayload = B.ByteString
type PacketSink = TMVar (PacketDirective, PacketPayload)
type StopSignal = TMVar Bool

parsePackets :: Handle -> PacketSink -> StopSignal -> IO ()
parsePackets !intakeHndl !pktSink !stopSignal = parsePkts B.empty
 where

  parsePkts :: B.ByteString -> IO ()
  parsePkts !readahead = do
    (payloadLen, directive, readahead') <- parsePktHdr readahead
    if payloadLen < 0
      then void $ atomically $ tryPutTMVar stopSignal True
      else do
        let (payload, rest) = B.splitAt payloadLen readahead'
            more2read       = payloadLen - B.length payload
        if more2read > 0
          then do
            morePayload <- B.hGet intakeHndl more2read
            atomically
                ((Right <$> putTMVar pktSink (directive, payload <> morePayload)
                 )
                `orElse` (Left <$> readTMVar stopSignal)
                )
              >>= \case
                    Left  _ -> return ()
                    Right _ -> parsePkts B.empty
          else
            atomically
                (        (Right <$> putTMVar pktSink (directive, payload))
                `orElse` (Left <$> readTMVar stopSignal)
                )
              >>= \case
                    Left  _ -> return ()
                    Right _ -> parsePkts rest

  parsePktHdr :: B.ByteString -> IO (Int, Text, B.ByteString)
  parsePktHdr !readahead = do
    peeked <- if B.null readahead
      then B.hGetSome intakeHndl maxHeaderLength
      else return readahead
    if B.null peeked
      then return (-1, "eos", B.empty)
      else do
        unless ("[" `B.isPrefixOf` peeked) $ throwIO $ userError
          "no packet header as expected"
        let (hdrPart, rest) = C.break (== ']') peeked
        if not $ B.null rest
          then do -- got a full packet header
            let !hdrContent         = B.drop 1 hdrPart
                !readahead'         = B.drop 1 rest
                (lenStr, directive) = C.break (== '#') hdrContent
                payloadLen          = read $ T.unpack $ decodeUtf8 lenStr
            return (payloadLen, decodeUtf8 $ B.drop 1 directive, readahead')
          else if B.length peeked < maxHeaderLength
            then do
              morePeek <- B.hGetSome intakeHndl maxHeaderLength
              parsePktHdr $ readahead <> morePeek
            else throwIO $ userError "packet header too long"

