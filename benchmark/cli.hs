{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main (main) where

import           Control.Applicative   ((<**>), (<|>))
import           Control.Concurrent    (forkIO, threadDelay)
import           Control.Concurrent.Chan
import           Control.Monad         (forever, void)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as BS
import           Data.ByteString.Char8 (pack)
import           Data.Char             (ord)
import           Data.Maybe            (fromJust)
import qualified Data.Text             as Text
import qualified Data.Text.Encoding    as Text
import           Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import qualified Data.Vector           as V
import           Data.Word             (Word64)
import qualified Network.HESP          as HESP
import qualified Network.HESP.Commands as HESP
import           Network.Socket        (HostName, Socket)
import           Numeric               (showFFloat)
import           Options.Applicative   (Parser)
import qualified Options.Applicative   as O
import           System.IO             (hPutStr, hSetBuffering)
import qualified System.IO             as SIO

-------------------------------------------------------------------------------

data App =
  App { connection :: ConnectionSetting
      , command    :: Command
      }

appOpts :: Parser App
appOpts = App <$> connectionOtpion <*> commandOtpion

main :: IO ()
main = do
  hSetBuffering SIO.stdout SIO.NoBuffering
  App{..} <- O.execParser $ O.info (appOpts <**> O.helper) O.fullDesc
  case command of
    SputCommand opts -> runTCPClient connection $ flip sput opts
    SgetCommand opts -> runTCPClient connection $ flip sget opts

runTCPClient :: ConnectionSetting -> (Socket -> IO a) -> IO a
runTCPClient ConnectionSetting{..} f =
  HESP.connect serverHost (show serverPort) $ \(sock, _) -> f sock

-------------------------------------------------------------------------------

data ConnectionSetting =
  ConnectionSetting { serverHost :: HostName
                    , serverPort :: Int
                    }

connectionOtpion :: Parser ConnectionSetting
connectionOtpion =
  ConnectionSetting
    <$> O.strOption (O.long "host"
                  <> O.short 'h'
                  <> O.showDefault
                  <> O.value "localhost"
                  <> O.metavar "HOST"
                  <> O.help "Server host")
    <*> O.option O.auto (O.long "port"
                      <> O.short 'p'
                      <> O.showDefault
                      <> O.value 6560
                      <> O.metavar "PORT"
                      <> O.help "Server port")

data Command = SputCommand SputOptions
             | SgetCommand SgetOptions

commandOtpion :: Parser Command
commandOtpion = SputCommand <$> O.hsubparser sputCommand
            <|> SgetCommand <$> O.hsubparser sgetCommand
  where
    sputCommand = O.command "sput" (O.info sputOptions (O.progDesc "SPUT -- publish messages to stream"))
    sgetCommand = O.command "sget" (O.info sgetOptions (O.progDesc "SGET -- fetch messages from stream"))

data SputOptions =
  SputOptions { numOfBytes :: Int
              , topicName  :: TopicName
              , pubLevel   :: Int
              , pubMethod  :: Int
              }

data SgetOptions =
  SgetOptions { numOfMessages :: Integer
              , topicNameFrom :: String
              , startID       :: Word64
              }

data TopicName = TopicName String | RandomTopicName

sputOptions :: Parser SputOptions
sputOptions =
  SputOptions
    <$> O.option O.auto (O.long "numOfBytes"
                      <> O.short 'b'
                      <> O.help "Number of bytes to be sent each times")
    <*> topicOption
    <*> O.option O.auto (O.long "pubLevel"
                      <> O.short 'l'
                      <> O.help "PubLevel, 0 or 1")
    <*> O.option O.auto (O.long "pubMethod"
                      <> O.short 'm'
                      <> O.help "PubMethod, 0 or 1")

sgetOptions :: Parser SgetOptions
sgetOptions =
  SgetOptions
    <$> O.option O.auto (O.long "numOfMessages"
                      <> O.short 'n'
                      <> O.help "Number of messages to be fetched each time")
    <*> O.strOption (O.long "topic"
                      <> O.help "Topic name")
    <*> O.option O.auto (O.long "startID"
                      <> O.short 's'
                      <> O.help "Message ID to start fetching from")

topicOption :: Parser TopicName
topicOption = a_topic <|> b_topic
  where
    a_topic = TopicName <$> O.strOption (O.long "topic" <> O.help "Topic Name")
    b_topic = O.flag' RandomTopicName (O.long "random-topic" <> O.help "Generate random topic name")

-------------------------------------------------------------------------------

sput :: Socket -> SputOptions -> IO ()
sput sock SputOptions{..} = do
  clientid <- preparePubRequest sock (fromIntegral pubLevel) (fromIntegral pubMethod)
  showSpeed "sput" numOfBytes (action clientid pubLevel)
  where
    action clientid level = do
      topic <- case topicName of
                 TopicName name  -> return $ encodeUtf8 name
                 RandomTopicName -> genTopic
      HESP.sendMsg sock $ sputRequest clientid topic payload
      case level of
        0 -> return ()
        1 -> do _ <- HESP.recvMsgs sock 1024
                -- TODO: assert result is OK
                return ()
        _ -> error "Invalid pubLevel."
    payload = BS.replicate numOfBytes (fromIntegral $ ord 'x')

sputRequest :: ByteString -> ByteString -> ByteString -> HESP.Message
sputRequest clientid topic payload =
  let cs = [ HESP.mkBulkString "sput"
           , HESP.mkBulkString clientid
           , HESP.mkBulkString topic
           , HESP.mkBulkString payload
           ]
   in HESP.mkArrayFromList cs

preparePubRequest :: Socket -> Integer -> Integer -> IO ByteString
preparePubRequest sock pubLevel pubMethod =
  let mapping = [ (HESP.mkBulkString "pub-level", HESP.Integer pubLevel)
                , (HESP.mkBulkString "pub-method", HESP.Integer pubMethod)
                ]
      hiCommand = HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                                       , HESP.mkMapFromList mapping
                                       ]
   in do HESP.sendMsg sock hiCommand
         resps <- HESP.recvMsgs sock 1024
         case resps V.! 0 of
           Left x  -> error x
           Right m -> return $ extractClientId m

-------------------------------------------------------------------------------
sget :: Socket -> SgetOptions -> IO ()
sget sock SgetOptions{..} = do
    clientid <- prepareSubRequest sock
    buffer <- newChan
    void . forkIO $ recvMsg sock buffer
    sendMsg sock buffer clientid (pack topicNameFrom)
      (pack . show $ startID) numOfMessages 0 0 0.0
  where
    recvMsg sock buffer = forever $ do
      datas <- HESP.recvMsgs sock 1024
      if V.null datas
        then error "Connection closed"
        else mapM_ (writeChan buffer) datas

sendMsg :: Socket
        -> Chan (Either a HESP.Message)
        -> ByteString -- Client ID
        -> ByteString -- Topic
        -> ByteString -- Start ID
        -> Integer    -- Num of Messages
        -> Integer    -- Offset
        -> Int        -- flow
        -> Double     -- time
        -> IO b
sendMsg sock buffer cid topic start maxn offset flow time = do
    startTime <- getPOSIXTime
    HESP.sendMsg sock $ prepareSGetRequest cid topic start offset
    _ <- HESP.recvMsgs sock 1024
    HESP.sendMsg sock $ prepareSGetcRequest cid topic maxn
    endTime   <- getPOSIXTime
    let deltaT = realToFrac $ endTime - startTime
    let time'  = time + deltaT
    go start flow time'
  where
    go start flow time = do
      startTime <- getPOSIXTime
      msg <- readChan buffer
      case msg of
        Right x ->
          case x of
            HESP.MatchPush "sgetc" args -> do
              let resp = fromJust $ HESP.getBulkStringParam args 2
              case resp of
                "OK" -> do
                  let entryid = fromJust $ HESP.getBulkStringParam args 3
                  let entrydata = fromJust $ HESP.getBulkStringParam args 4
                      entrydataBytes = BS.length entrydata
                  endTime   <- getPOSIXTime
                  let flow' = flow + entrydataBytes
                  showSpeed' time startTime endTime flow' (go entryid)
                "DONE" -> do
                  HESP.sendMsg sock $ prepareSGetcRequest cid topic maxn
                  endTime   <- getPOSIXTime
                  showSpeed' time startTime endTime flow (go start)
                "END" -> do
                  let offset' = if BS.null start then 0 else 1
                  threadDelay 1000000
                  endTime   <- getPOSIXTime
                  showSpeed' time startTime endTime flow (sendMsg sock buffer cid topic start maxn offset')
                "ERR" -> do
                  let errmsg = args V.! 2
                  error $ show errmsg
                _     -> error "unexpected response"
            _                           -> error "unexpected response"
        Left _ -> undefined

showSpeed' :: Double -> POSIXTime -> POSIXTime -> Int -> (Int -> Double -> IO a) -> IO a
showSpeed' time startTime endTime flow contAction = do
  let deltaT = realToFrac $ endTime - startTime
  let time'  = time + deltaT
  if time' > 1
    then do
      let speed = fromIntegral flow / time' / 1024 / 1024
      hPutStr SIO.stdout $ "\r=> " <> "sget" <> " speed: "
                        <> showFFloat (Just 2) speed " MiB/s"
                        <> " (" <> show flow <> " bytes in "
                        <> showFFloat (Just 5) time' " seconds)"
      contAction 0 0.0
    else contAction flow time'

prepareSubRequest :: Socket -> IO ByteString
prepareSubRequest sock =
  let hiCommand = HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                                       , HESP.mkMapFromList []
                                       ]
   in do HESP.sendMsg sock hiCommand
         resps <- HESP.recvMsgs sock 1024
         case resps V.! 0 of
           Left x  -> error x
           Right m -> return $ extractClientId m

prepareSGetRequest :: ByteString -> ByteString -> ByteString -> Integer -> HESP.Message
prepareSGetRequest cid topic start offset =
  let cs =
        [ HESP.mkBulkString "sget",
          HESP.mkBulkString cid,
          HESP.mkBulkString topic,
          HESP.mkBulkString start,
          HESP.mkBulkString "",
          HESP.Integer offset
        ]
   in HESP.mkArrayFromList cs

prepareSGetcRequest :: ByteString -> ByteString -> Integer -> HESP.Message
prepareSGetcRequest cid topic maxn =
  let cs =
        [ HESP.mkBulkString "sgetc",
          HESP.mkBulkString cid,
          HESP.mkBulkString topic,
          HESP.Integer maxn
        ]
   in HESP.mkArrayFromList cs

-------------------------------------------------------------------------------
extractClientId :: HESP.Message -> ByteString
extractClientId (HESP.MatchArray vs) =
  let t = fromJust $ HESP.getBulkStringParam vs 0
      r = fromJust $ HESP.getBulkStringParam vs 1
      i = fromJust $ HESP.getBulkStringParam vs 2
   in if t == "hi" && r == "OK" then i else error "clientid error"
extractClientId x = error $ "Unexpected message: " <> show x

showSpeed :: String -> Int -> IO a -> IO b
showSpeed label numBytes action = go 0 0.0
  where
    go :: Int -> Double -> IO b
    go flow time = do
      startTime <- getPOSIXTime
      _ <- action
      endTime <- getPOSIXTime
      let deltaT = realToFrac $ endTime - startTime
      let flow' = flow + numBytes
      let time' = time + deltaT
      if time' > 1
         then do let speed = (fromIntegral flow') / time' / 1024 / 1024
                 hPutStr SIO.stdout $ "\r=> " <> label <> " speed: "
                                   <> showFFloat (Just 2) speed " MiB/s"
                 go 0 0.0
         else go flow' time'

genTopic :: IO ByteString
genTopic = do
  x <- floor <$> getPOSIXTime :: IO Int
  return $ "topic-" <> (encodeUtf8 . show) x

encodeUtf8 :: String -> ByteString
encodeUtf8 = Text.encodeUtf8 . Text.pack
