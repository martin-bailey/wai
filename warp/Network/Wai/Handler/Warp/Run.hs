{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-}

module Network.Wai.Handler.Warp.Run where

import Control.Arrow (first)
import Control.Concurrent (threadDelay, forkIOWithUnmask)
import qualified Control.Concurrent as Conc (yield)
import Control.Exception as E
import Control.Monad (when, unless, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Streaming.Network (bindPortTCP)
import Network (sClose, Socket)
import Network.Socket (accept, withSocketsDo, SockAddr)
import qualified Network.Socket.ByteString as Sock
import Network.Wai
import Network.Wai.Handler.Warp.Buffer
import Network.Wai.Handler.Warp.Counter
import qualified Network.Wai.Handler.Warp.Date as D
import qualified Network.Wai.Handler.Warp.FdCache as F
import Network.Wai.Handler.Warp.Header
import Network.Wai.Handler.Warp.Recv
import Network.Wai.Handler.Warp.Request
import Network.Wai.Handler.Warp.Response
import Network.Wai.Handler.Warp.SendFile
import Network.Wai.Handler.Warp.Settings
import qualified Network.Wai.Handler.Warp.Timeout as T
import Network.Wai.Handler.Warp.Types
import Network.Wai.Internal (ResponseReceived (ResponseReceived))
import System.IO.Error (isFullErrorType, ioeGetErrorType)

#if WINDOWS
import Network.Wai.Handler.Warp.Windows
#else
import System.Posix.IO (FdOption(CloseOnExec), setFdOption)
import Network.Socket (fdSocket)
#endif

-- | Default action value for 'Connection'.
socketConnection :: Socket -> IO Connection
socketConnection s = do
    bufferPool <- newBufferPool
    readBuf <- allocateBuffer 0 -- kept for backward compatibility
    writeBuf <- allocateBuffer bufferSize
    return Connection {
        connSendMany = Sock.sendMany s
      , connSendAll = Sock.sendAll s
      , connSendFile = defaultSendFile s
      , connClose = sClose s >> freeBuffer writeBuf
      , connRecv = receive s bufferPool
      , connBufferPool = bufferPool
      , connReadBuffer = readBuf
      , connWriteBuffer = writeBuf
      , connBufferSize = bufferSize
      , connSendFileOverride = Override s
      }

#if __GLASGOW_HASKELL__ < 702
allowInterrupt :: IO ()
allowInterrupt = unblock $ return ()
#endif

-- | Run an 'Application' on the given port. This calls 'runSettings' with
-- 'defaultSettings'.
run :: Port -> Application -> IO ()
run p = runSettings defaultSettings { settingsPort = p }

-- | Run an 'Application' with the given 'Settings'.
runSettings :: Settings -> Application -> IO ()
runSettings set app = withSocketsDo $
    bracket
        (bindPortTCP (settingsPort set) (settingsHost set))
        sClose
        (\socket -> do
            setSocketCloseOnExec socket
            runSettingsSocket set socket app)

-- | Same as 'runSettings', but uses a user-supplied socket instead of opening
-- one. This allows the user to provide, for example, Unix named socket, which
-- can be used when reverse HTTP proxying into your application.
--
-- Note that the 'settingsPort' will still be passed to 'Application's via the
-- 'serverPort' record.
--
-- When the listen socket in the second argument is closed, all live
-- connections are gracefully shut-downed.
runSettingsSocket :: Settings -> Socket -> Application -> IO ()
runSettingsSocket set socket app = do
    settingsInstallShutdownHandler set closeListenSocket
    runSettingsConnection set getConn app
  where
    getConn = do
#if WINDOWS
        (s, sa) <- windowsThreadBlockHack $ accept socket
#else
        (s, sa) <- accept socket
#endif
        setSocketCloseOnExec s
        conn <- socketConnection s
        return (conn, sa)

    closeListenSocket = sClose socket

-- | Allows you to provide a function which will return a 'Connection'. In
-- cases where creating the @Connection@ can be expensive, this allows the
-- expensive computations to be performed in a separate thread instead of the
-- main server loop.
--
-- Since 1.3.5
runSettingsConnection :: Settings -> IO (Connection, SockAddr) -> Application -> IO ()
runSettingsConnection set getConn app = runSettingsConnectionMaker set getConnMaker app
  where
    getConnMaker = do
      (conn, sa) <- getConn
      return (return conn, sa)

runSettingsConnectionMaker :: Settings -> IO (IO Connection, SockAddr) -> Application -> IO ()
runSettingsConnectionMaker x y =
    runSettingsConnectionMakerSecure x (go y)
  where
    go = fmap (first (fmap (, False)))

----------------------------------------------------------------

-- | Allows you to provide a function which will return a function
-- which will return 'Connection'.
--
-- Since 2.1.4
runSettingsConnectionMakerSecure :: Settings -> IO (IO (Connection, Bool), SockAddr) -> Application -> IO ()
runSettingsConnectionMakerSecure set getConnMaker app = do
    settingsBeforeMainLoop set
    counter <- newCounter

    D.withDateCache $ \dc ->
        F.withFdCache fdCacheDurationInSeconds $ \fc ->
            withTimeoutManager $ \tm ->
                acceptConnection set getConnMaker app dc fc tm counter
  where
    fdCacheDurationInSeconds = settingsFdCacheDuration set * 1000000
    withTimeoutManager f = case settingsManager set of
        Just tm -> f tm
        Nothing -> bracket
                   (T.initialize $ settingsTimeout set * 1000000)
                   T.stopManager
                   f

onE :: Settings -> Maybe Request -> SomeException -> IO ()
onE set mreq e = case fromException e of
    Just (NotEnoughLines []) -> return ()
    _                        -> settingsOnException set mreq e

-- Note that there is a thorough discussion of the exception safety of the
-- following code at: https://github.com/yesodweb/wai/issues/146
--
-- We need to make sure of two things:
--
-- 1. Asynchronous exceptions are not blocked entirely in the main loop.
--    Doing so would make it impossible to kill the Warp thread.
--
-- 2. Once a connection maker is received via acceptNewConnection, the
--    connection is guaranteed to be closed, even in the presence of
--    async exceptions.
--
-- Our approach is explained in the comments below.
acceptConnection :: Settings
                 -> IO (IO (Connection, Bool), SockAddr)
                 -> Application
                 -> D.DateCache
                 -> Maybe F.MutableFdCache
                 -> T.Manager
                 -> Counter
                 -> IO ()
acceptConnection set getConnMaker app dc fc tm counter = do
    -- First mask all exceptions in acceptLoop. This is necessary to
    -- ensure that no async exception is throw between the call to
    -- acceptNewConnection and the registering of connClose.
    void $ mask_ $ acceptLoop
    gracefulShutdown counter
  where
    acceptLoop = do
        -- Allow async exceptions before receiving the next connection maker.
        allowInterrupt

        -- acceptNewConnection will try to receive the next incoming
        -- request. It returns a /connection maker/, not a connection,
        -- since in some circumstances creating a working connection
        -- from a raw socket may be an expensive operation, and this
        -- expensive work should not be performed in the main event
        -- loop. An example of something expensive would be TLS
        -- negotiation.
        mx <- acceptNewConnection
        case mx of
            Nothing             -> return ()
            Just (mkConn, addr) -> do
                fork set mkConn addr app dc fc tm counter
                acceptLoop

    acceptNewConnection = do
        ex <- try getConnMaker
        case ex of
            Right x -> return $ Just x
            Left  e  -> do
                onE set Nothing $ toException e
                if isFullErrorType (ioeGetErrorType e) then do
                    -- "resource exhausted (Too many open files)" may
                    -- happen by accept().  Wait a second hoping that
                    -- resource will be available.
                    threadDelay 1000000
                    acceptNewConnection
                  else
                    -- Assuming the listen socket is closed.
                    return Nothing

-- Fork a new worker thread for this connection maker, and ask for a
-- function to unmask (i.e., allow async exceptions to be thrown).
fork :: Settings
     -> IO (Connection, Bool)
     -> SockAddr
     -> Application
     -> D.DateCache
     -> Maybe F.MutableFdCache
     -> T.Manager
     -> Counter
     -> IO ()
fork set mkConn addr app dc fc tm counter = void $ forkIOWithUnmask $ \unmask ->
    -- Run the connection maker to get a new connection, and ensure
    -- that the connection is closed. If the mkConn call throws an
    -- exception, we will leak the connection. If the mkConn call is
    -- vulnerable to attacks (e.g., Slowloris), we do nothing to
    -- protect the server. It is therefore vital that mkConn is well
    -- vetted.
    --
    -- We grab the connection before registering timeouts since the
    -- timeouts will be useless during connection creation, due to the
    -- fact that async exceptions are still masked.
    bracket mkConn closeConn $ \(conn0, isSecure') ->

    -- We need to register a timeout handler for this thread, and
    -- cancel that handler as soon as we exit.
    bracket (T.registerKillThread tm) T.cancel $ \th ->

    let ii = InternalInfo th fc dc
        conn = setSendFile conn0 fc
        -- We now have fully registered a connection close handler
        -- in the case of all exceptions, so it is safe to one
        -- again allow async exceptions.
    in unmask .
       -- Call the user-supplied on exception code if any
       -- exceptions are thrown.
       handle (onE set Nothing) .

       -- Call the user-supplied code for connection open and close events
       bracket (onOpen addr) (onClose addr) $ \goingon ->

       -- Actually serve this connection.
       -- onnClose above ensures the termination of the connection.
       when goingon $ serveConnection conn ii addr isSecure' set app
  where
    closeConn (conn, _isSecure) = connClose conn

    onOpen adr    = increase counter >> settingsOnOpen  set adr
    onClose adr _ = decrease counter >> settingsOnClose set adr

serveConnection :: Connection
                -> InternalInfo
                -> SockAddr
                -> Bool -- ^ is secure?
                -> Settings
                -> Application
                -> IO ()
serveConnection conn ii addr isSecure' settings app = do
    istatus <- newIORef False
    src <- mkSource (connSource conn th istatus)
    recvSendLoop istatus src `E.catch` \e -> do
        sendErrorResponse istatus e
        throwIO (e :: SomeException)

  where
    th = threadHandle ii

    sendErrorResponse istatus e = do
        status <- readIORef istatus
        when status $ void $
            sendResponse conn ii dummyreq defaultIndexRequestHeader (return S.empty) (errorResponse e)

    dummyreq = defaultRequest { remoteHost = addr }

    errorResponse e = settingsOnExceptionResponse settings e

    recvSendLoop istatus fromClient = do
        (req', idxhdr) <- recvRequest settings conn ii addr fromClient
        let req = req' { isSecure = isSecure' }
        -- Let the application run for as long as it wants
        T.pause th

        -- In the event that some scarce resource was acquired during
        -- creating the request, we need to make sure that we don't get
        -- an async exception before calling the ResponseSource.
        keepAliveRef <- newIORef $ error "keepAliveRef not filled"
        _ <- app req $ \res -> do
            T.resume th
            -- FIXME consider forcing evaluation of the res here to
            -- send more meaningful error messages to the user.
            -- However, it may affect performance.
            writeIORef istatus False
            keepAlive <- sendResponse conn ii req idxhdr (readSource fromClient) res
            writeIORef keepAliveRef keepAlive
            return ResponseReceived
        keepAlive <- readIORef keepAliveRef

        -- We just send a Response and it takes a time to
        -- receive a Request again. If we immediately call recv,
        -- it is likely to fail and the IO manager works.
        -- It is very costy. So, we yield to another Haskell
        -- thread hoping that the next Request will arraive
        -- when this Haskell thread will be re-scheduled.
        -- This improves performance at least when
        -- the number of cores is small.
        Conc.yield

        when keepAlive $ do
            -- flush the rest of the request body
            flushBody $ requestBody req
            T.resume th
            recvSendLoop istatus fromClient

flushBody :: IO ByteString -> IO ()
flushBody src =
    loop
  where
    loop = do
        bs <- src
        unless (S.null bs) loop

connSource :: Connection -> T.Handle -> IORef Bool -> IO ByteString
connSource Connection { connRecv = recv } th istatus = do
    bs <- recv
    unless (S.null bs) $ do
        writeIORef istatus True
        when (S.length bs >= 2048) $ T.tickle th
    return bs

-- Copied from: https://github.com/mzero/plush/blob/master/src/Plush/Server/Warp.hs
setSocketCloseOnExec :: Socket -> IO ()
#if WINDOWS
setSocketCloseOnExec _ = return ()
#else
setSocketCloseOnExec socket =
    setFdOption (fromIntegral $ fdSocket socket) CloseOnExec True
#endif

gracefulShutdown :: Counter -> IO ()
gracefulShutdown counter = do
    -- To avoid race condition, we just use threadDelay, not MVar.
    threadDelay 10000000
    noConnections <- isZero counter
    unless noConnections $ gracefulShutdown counter
