{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Mangrove.Server
  ( onRecvMsg
  ) where

import qualified Colog
import           Control.Exception        (SomeException, throw, try)
import           Control.Monad.Reader     (ask, liftIO)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString.Char8    as BSC
import qualified Data.ByteString.Lazy     as LBS
import           Data.Map.Strict          (Map)
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8)
import           Data.Vector              (Vector)
import qualified Data.Vector              as V
import           Data.Word                (Word64)
import           Network.Socket           (Socket)
import           Text.Read                (readMaybe)

import           Log.Store.Base           (Context, EntryID,
                                           LogStoreException (..))
import qualified Mangrove.Server.Response as I
import qualified Mangrove.Store           as Store
import qualified Mangrove.Streaming       as S
import           Mangrove.Types           (App, ClientId, Env (..))
import qualified Mangrove.Types           as T
import           Mangrove.Utils           ((.|.))
import qualified Mangrove.Utils           as U
import qualified Network.HESP             as HESP
import qualified Network.HESP.Commands    as HESP

-------------------------------------------------------------------------------

-- | Client request types
data RequestType
  = Handshake (Map HESP.Message HESP.Message)
  | SPut ClientId ByteString ByteString
  | SPuts ClientId ByteString (V.Vector ByteString)
  | SGet ClientId ByteString (Maybe Word64) (Maybe Word64) Integer
  | SGetCtrl ClientId ByteString Integer
  | SRange ClientId ByteString (Maybe Word64) (Maybe Word64) Integer Integer
  | XAdd ByteString ByteString
  | XRange ByteString (Maybe Word64) (Maybe Word64) (Maybe Integer)
  deriving (Show, Eq)

-- | Parse client request and then send response to client.
onRecvMsg :: Socket
          -> Context
          -> Either String HESP.Message
          -> App (Maybe ())
onRecvMsg sock _ (Left errmsg) = do
  Colog.logError $ "Failed to parse message: " <> Text.pack errmsg
  HESP.sendMsg sock $ I.mkGeneralError $ U.str2bs errmsg
  -- Outer function will catch all exceptions and do a clean job.
  errorWithoutStackTrace "Invalid TCP message!"
onRecvMsg sock ctx (Right msg) =
  case parseRequest msg of
    Left e    -> do
      Colog.logWarning $ decodeUtf8 e
      HESP.sendMsg sock $ I.mkGeneralError e
      return Nothing
    Right req -> processRequest sock ctx req

parseRequest :: HESP.Message
             -> Either ByteString RequestType
parseRequest msg = do
  (n, paras) <- HESP.commandParser msg
  case n of
    "hi"     -> parseHandshake paras
    "sput"   -> parseSPuts paras <> parseSPut paras
    "sget"   -> parseSGet paras
    "sgetc"  -> parseSGetCtrl paras
    "srange" -> parseSRange paras
    "xadd"   -> parseXAdd paras
    "xrange" -> parseXRange paras
    _        -> Left $ "Unrecognized request " <> n <> "."

processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processHandshake sock ctx rt
                         .|. processSPut      sock ctx rt
                         .|. processSPuts     sock ctx rt
                         .|. processSGet      sock ctx rt
                         .|. processSGetCtrl  sock rt
                         .|. processSRange    sock ctx rt
                         .|. processXAdd      sock ctx rt
                         .|. processXRange    sock ctx rt

-------------------------------------------------------------------------------
-- Parse client requests

parseHandshake :: Vector HESP.Message -> Either ByteString RequestType
parseHandshake paras = do
  options <- HESP.extractMapParam "Options" paras 0
  return $ Handshake options

parseSPut :: Vector HESP.Message -> Either ByteString RequestType
parseSPut paras = do
  cidStr  <- HESP.extractBulkStringParam "Client ID" paras 0
  cid     <- T.getClientIdFromASCIIBytes' cidStr
  topic   <- HESP.extractBulkStringParam "Topic"     paras 1
  payload <- HESP.extractBulkStringParam "Payload"   paras 2
  return $ SPut cid topic payload

parseSPuts :: Vector HESP.Message
           -> Either ByteString RequestType
parseSPuts paras = do
  cidStr   <- HESP.extractBulkStringParam      "Client ID" paras 0
  cid      <- T.getClientIdFromASCIIBytes' cidStr
  topic    <- HESP.extractBulkStringParam      "Topic"     paras 1
  payloads <- HESP.extractBulkStringArrayParam "Payload"   paras 2
  return $ SPuts cid topic payloads

parseSGet :: Vector HESP.Message -> Either ByteString RequestType
parseSGet paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID" paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  topic  <- HESP.extractBulkStringParam "Topic"     paras 1
  sids   <- HESP.extractBulkStringParam "Start ID"  paras 2
  sid    <- validateInt                 "Start ID"  sids ""
  eids   <- HESP.extractBulkStringParam "End ID"    paras 3
  eid    <- validateInt                 "End ID"    eids ""
  offset <- HESP.extractIntegerParam    "Offset"    paras 4
  return $ SGet cid topic sid eid offset

parseSGetCtrl :: Vector HESP.Message -> Either ByteString RequestType
parseSGetCtrl paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID"          paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  topic  <- HESP.extractBulkStringParam "Topic"              paras 1
  maxn   <- HESP.extractIntegerParam    "Max message number" paras 2
  return $ SGetCtrl cid topic maxn

parseSRange :: Vector HESP.Message -> Either ByteString RequestType
parseSRange paras = do
  cidStr <- HESP.extractBulkStringParam "Client ID" paras 0
  cid    <- T.getClientIdFromASCIIBytes' cidStr
  topic  <- HESP.extractBulkStringParam "Topic"     paras 1
  sids   <- HESP.extractBulkStringParam "Start ID"  paras 2
  sid    <- validateInt                 "Start ID"  sids ""
  eids   <- HESP.extractBulkStringParam "End ID"    paras 3
  eid    <- validateInt                 "End ID"    eids ""
  offset <- HESP.extractIntegerParam    "Offset"    paras 4
  maxn <- HESP.extractIntegerParam      "Maxn"      paras 5
  return $ SRange cid topic sid eid offset maxn

parseXAdd :: Vector HESP.Message -> Either ByteString RequestType
parseXAdd paras = do
  topic <- HESP.extractBulkStringParam "Topic" paras 0
  reqid <- HESP.extractBulkStringParam "Entry ID" paras 1
  _     <- validateBSSimple "*" reqid "Invalid stream ID specified as stream command argument"
  let kvs = V.drop 2 paras
  case V.length kvs > 0 && even (V.length kvs) of
    -- XXX: separate payload generating process
    True  -> return $ XAdd topic (HESP.serialize $ HESP.mkArray kvs)
    False -> Left "wrong number of arguments for 'xadd' command"

parseXRange :: Vector HESP.Message -> Either ByteString RequestType
parseXRange paras = do
  topic <- HESP.extractBulkStringParam "Topic"     paras 0
  sids  <- HESP.extractBulkStringParam "Start ID"  paras 1
  sid   <- validateIntSimple sids "-" "Invalid stream ID specified as stream command argument"
  eids  <- HESP.extractBulkStringParam "End ID"    paras 2
  eid   <- validateIntSimple eids "+" "Invalid stream ID specified as stream command argument"
  case V.length paras of
    3 -> return $ XRange topic sid eid Nothing
    5 -> do
      count  <- HESP.extractBulkStringParam "Option" paras 3
      _      <- validateBSSimple "count" count "syntax error"
      maxns  <- HESP.extractBulkStringParam "Maxn"   paras 4
      e_maxn <- validateIntSimple maxns "" "value is not an integer or out of range"
      case e_maxn of
        Just n -> return $ XRange topic sid eid (Just $ toInteger n)
        _      -> Left "value is not an integer or out of range"
    _ -> Left "syntax error"

-------------------------------------------------------------------------------
-- Process client requests

processHandshake :: Socket -> Context -> RequestType -> App (Maybe ())
processHandshake sock _ (Handshake opt) = do
  Env{..} <- ask
  cid <- liftIO T.newClientId
  client <- liftIO $ T.newClientStatus sock opt
  liftIO $ T.insertClient cid client serverStatus
  Colog.logInfo $ "Created client: " <> T.packClientId cid
  let resp = I.mkHandshakeRespSucc cid
  Colog.logDebug $ Text.pack ("Sending: " ++ show resp)
  HESP.sendMsg sock resp
  return $ Just ()
processHandshake _ _ _ = return Nothing

processSPut :: Socket -> Context -> RequestType -> App (Maybe ())
processSPut sock ctx (SPut cid topic payload) =
  let payloads = V.singleton payload
   in pub sock ctx "sput" cid topic payloads >> (return $ Just ())
processSPut _ _ _ = return Nothing

processSPuts :: Socket -> Context -> RequestType -> App (Maybe ())
processSPuts sock ctx (SPuts cid topic payloads) =
  pub sock ctx "sput" cid topic payloads >> (return $ Just ())
processSPuts _ _ _ = return Nothing

processSGet :: Socket -> Context -> RequestType -> App (Maybe ())
processSGet sock ctx (SGet cid topic sid eid offset) = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError "sget" errmsg
    Just client -> do
      Colog.logDebug $ "Reading " <> decodeUtf8 topic <> " ..."
      let clientSock = T.clientSocket client
      -- NOTE: offset must not exceed maxBound::Int
      presget clientSock ctx "sget" cid topic sid eid (fromInteger offset)
  return $ Just ()
processSGet _ _ _ = return Nothing

processSGetCtrl :: Socket -> RequestType -> App (Maybe ())
processSGetCtrl sock (SGetCtrl cid topic maxn) =
  sgetctrl sock "sgetc" cid topic (fromInteger maxn) >> return (Just ())
processSGetCtrl _ _ = return Nothing

processSRange :: Socket -> Context -> RequestType -> App (Maybe ())
processSRange sock ctx (SRange cid topic sid eid offset maxn) = do
  let lcmd = "srange"
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      let cut = S.take (fromInteger maxn) . S.drop (fromInteger offset)
      e_s <- liftIO . try $ cut <$> Store.readStreamEntry ctx topic sid eid
      let clientSock = T.clientSocket client
      case e_s of
        Left e@(LogStoreLogNotFoundException _) -> do
          let errmsg = "Topic " <> topic <> " not found."
          Colog.logError . Text.pack . show $ e
          HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg
        Left e -> throw e
        Right s -> do
          let enc = HESP.serialize . I.mkElementResp cid lcmd topic
          resp <- liftIO $ S.encodeFromChunksIO enc s
          HESP.sendLazy clientSock resp
          HESP.sendMsg clientSock $ I.mkCmdPush cid lcmd topic "DONE"
  return $ Just ()
processSRange _ _ _ = return Nothing

processXAdd :: Socket -> Context -> RequestType -> App (Maybe ())
processXAdd sock ctx (XAdd topic payload) = do
  r <- liftIO $ Store.sput ctx topic payload
  case r of
    Right entryID -> do
      liftIO $ HESP.sendMsg sock $ (HESP.mkBulkString . U.str2bs . show) entryID
    Left (e :: SomeException) -> do
      let errmsg = "Message storing failed: " <> (U.str2bs . show) e
      Colog.logWarning $ decodeUtf8 errmsg
      liftIO $ HESP.sendMsg sock $ I.mkGeneralError errmsg
  return $ Just ()
processXAdd _ _ _ = return Nothing

processXRange :: Socket -> Context -> RequestType -> App (Maybe ())
processXRange sock ctx (XRange topic sid eid maxn) = do
  let cut = case maxn of
        Nothing -> id
        Just n  -> S.take (fromInteger n)
  e_s <- liftIO . try $ cut <$> Store.readStreamEntry ctx topic sid eid
  case e_s of
    Left (LogStoreLogNotFoundException _) -> do
      HESP.sendMsg sock $ HESP.mkArrayFromList []
    Left e  -> throw e
    Right s -> do
      elems <- liftIO $ S.toList s
      let respMsg = HESP.mkArrayFromList $ I.mkSimpleElementResp <$> elems
      HESP.sendMsg sock respMsg
  return $ Just ()
processXRange _ _ _ = return Nothing

-------------------------------------------------------------------------------

presget :: Socket
        -> Context
        -> ByteString
        -> ClientId
        -> ByteString
        -> Maybe EntryID
        -> Maybe EntryID
        -> Int
        -> App ()
presget sock ctx lcmd cid topic sid eid offset = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      e_s <- liftIO . try $ S.drop offset <$> Store.readStreamEntry ctx topic sid eid
      case e_s of
        Left e@(LogStoreLogNotFoundException _) -> do
          let errmsg = "Topic " <> topic <> " not found."
          Colog.logError . Text.pack . show $ e
          HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
        Left e -> throw e
        Right s -> do
          is_succ <- liftIO $ T.tryInsertConsumeStream topic s client
          let resp = case is_succ of
                       False -> I.mkCmdPushError cid lcmd topic "already existed"
                       True  -> I.mkCmdPush cid lcmd topic "OK"
          HESP.sendMsg sock resp

sgetctrl :: Socket -> ByteString -> ClientId -> ByteString -> Int -> App ()
sgetctrl sock lcmd cid topic maxn = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logError $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      mes <- liftIO $ T.takeConsumeElements client topic maxn
      case mes of
        Nothing -> do
          let errmsg = "elements not found"
          HESP.sendMsg sock $ I.mkCmdPushError cid lcmd topic errmsg
        Just es -> do
          let enc = HESP.serialize . I.mkElementResp cid lcmd topic
          -- FIXME: If SomeException happens, the client socket will be closed
          -- immediately. There is no error message send to client.
          rbs <- liftIO $ S.encodeFromChunksIO enc es
          if LBS.null rbs
             then do liftIO $ T.deleteClientConsume topic client
                     HESP.sendMsg sock $ I.mkCmdPush cid lcmd topic "END"
             -- TODO: sub-level
             else do HESP.sendLazy sock rbs
                     HESP.sendMsg sock $ I.mkCmdPush cid lcmd topic "DONE"

pub :: Socket
    -> Context
    -> ByteString
    -> ClientId
    -> ByteString
    -> Vector ByteString
    -> App ()
pub sock ctx lcmd cid topic payloads = do
  Env{serverStatus = serverStatus} <- ask
  m_client <- liftIO $ T.getClient cid serverStatus
  case m_client of
    Nothing -> do
      let errmsg = "ClientID " <> T.packClientIdBS cid <> " not found."
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
    Just client -> do
      Colog.logDebug $ "Writing " <> decodeUtf8 topic <> " ..."
      let e_level  = T.extractClientPubLevel  client
          e_method = T.extractClientPubMethod client
      let clientSock = T.clientSocket client
      case e_method of
        Right 0 -> do
          r <- liftIO $ Store.sputsAtom ctx topic payloads
          pubRespByLevel clientSock lcmd cid topic e_level r
        Right 1 -> do
          r <- liftIO $ Store.sputs ctx topic payloads
          pubRespByLevel clientSock lcmd cid topic e_level r
        Right x -> do
          let errmsg = "Unsupported pub-method: " <> (U.str2bs . show) x
          Colog.logWarning $ decodeUtf8 errmsg
          HESP.sendMsg sock $ I.mkGeneralPushError lcmd errmsg
        Left x -> do
          let errmsg = "Extract pub-method error: " <> (U.str2bs . show) x
          Colog.logWarning $ decodeUtf8 errmsg
          HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg

pubRespByLevel :: Socket
               -> ByteString
               -> ClientId
               -> ByteString
               -> Either ByteString Integer
               -> Either (Vector EntryID, SomeException) (Vector EntryID)
               -> App ()
pubRespByLevel clientSock lcmd cid topic level r =
  case level of
    Right 0 -> return ()
    Right 1 -> case r of
      Left (entryIDs, e) -> do
        Colog.logException e
        let succIds = V.map (I.mkSPutRespSucc cid topic) entryIDs
            errResp = I.mkSPutRespFail cid topic "Message storing failed."
        let resps = V.snoc succIds errResp
        Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
        HESP.sendMsgs clientSock resps
      Right entryIDs     -> do
        let resps = V.map (I.mkSPutRespSucc cid topic) entryIDs
        Colog.logDebug $ Text.pack ("Sending: " ++ show resps)
        HESP.sendMsgs clientSock resps
    Right x -> do
      let errmsg = "Unsupported pub-level: " <> (U.str2bs . show) x
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg
    Left x  -> do
      let errmsg = "Extract pub-level error: " <> (U.str2bs . show) x
      Colog.logWarning $ decodeUtf8 errmsg
      HESP.sendMsg clientSock $ I.mkGeneralPushError lcmd errmsg

-------------------------------------------------------------------------------
-- Helpers

validateInt :: ByteString -> ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateInt label s def = validateIntSimple s def (label <> " must be an integer")

validateIntSimple :: ByteString -> ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateIntSimple s def errMsg
  | s == def   = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left errMsg
      Just x  -> Right (Just x)

validateBSSimple :: ByteString -> ByteString -> ByteString -> Either ByteString ByteString
validateBSSimple expected real errMsg
  | real == expected = Right real
  | otherwise = Left errMsg
