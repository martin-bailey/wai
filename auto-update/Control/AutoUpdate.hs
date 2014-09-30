{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RecordWildCards    #-}

-- | A common problem is the desire to have an action run at a scheduled
-- interval, but only if it is needed. For example, instead of having
-- every web request result in a new @getCurrentTime@ call, we'd like to
-- have a single worker thread run every second, updating an @IORef@.
-- However, if the request frequency is less than once per second, this is
-- a pessimization, and worse, kills idle GC.
--
-- This library allows you to define actions which will either be
-- performed by a dedicated thread or, in times of low volume, will be
-- executed by the calling thread.
module Control.AutoUpdate (
      -- * Type
      UpdateSettings
    , defaultUpdateSettings
      -- * Accessors
    , updateFreq
    , updateSpawnThreshold
    , updateAction
      -- * Creation
    , mkAutoUpdate
    ) where

import           Control.AutoUpdate.Util (atomicModifyIORef')
import           Control.Concurrent (ThreadId, forkIO, myThreadId, threadDelay)
import           Control.Exception  (Exception, SomeException
                                    ,assert, fromException, handle,throwIO, throwTo)
import           Control.Monad      (forever, join)
import           Data.IORef         (IORef, newIORef, readIORef, writeIORef)
import           Data.Typeable      (Typeable)

-- | Default value for creating an @UpdateSettings@.
--
-- Since 0.1.0
defaultUpdateSettings :: UpdateSettings ()
defaultUpdateSettings = UpdateSettings
    { updateFreq = 1000000
    , updateSpawnThreshold = 3
    , updateAction = return ()
    }

-- | Settings to control how values are updated.
--
-- This should be constructed using @defaultUpdateSettings@ and record
-- update syntax, e.g.:
--
-- @
-- let set = defaultUpdateSettings { updateAction = getCurrentTime }
-- @
--
-- Since 0.1.0
data UpdateSettings a = UpdateSettings
    { updateFreq           :: Int
    -- ^ Microseconds between update calls. Same considerations as
    -- @threadDelay@ apply.
    --
    -- Default: 1 second (1000000)
    --
    -- Since 0.1.0
    , updateSpawnThreshold :: Int
    -- ^ How many times the data must be requested before we decide to
    -- spawn a dedicated thread.
    --
    -- Default: 3
    --
    -- Since 0.1.0
    , updateAction         :: IO a
    -- ^ Action to be performed to get the current value.
    --
    -- Default: does nothing.
    --
    -- Since 0.1.0
    }

data Status a = AutoUpdated
                    !a
                    (IORef Int)
                    -- Number of times used since last updated.
                    {-# UNPACK #-} !ThreadId
                    -- Worker thread.
              | ManualUpdates
                    (IORef Int)
                    -- Number of times used since we started/switched
                    -- off manual updates.

-- | Generate an action which will either read from an automatically
-- updated value, or run the update action in the current thread.
--
-- Since 0.1.0
mkAutoUpdate :: UpdateSettings a -> IO (IO a)
mkAutoUpdate us = do
    counter <- newIORef 0
    istatus <- newIORef $ ManualUpdates $ counter
    return $! getCurrent us istatus

data Action a = Return a | Manual | Spawn

data Replaced = Replaced deriving (Show, Typeable)
instance Exception Replaced

-- | Get the current value, either fed from an auto-update thread, or
-- computed manually in the current thread.
--
-- Since 0.1.0
getCurrent :: UpdateSettings a
           -> IORef (Status a) -- ^ mutable state
           -> IO a
getCurrent settings@UpdateSettings{..} istatus = do
--    status <- readIORef istatus
    ea <- increment istatus
    case ea of
        Return a -> return a
        Manual   -> updateAction
        Spawn    -> do
            a <- updateAction
            tid <- forkIO $ spawn settings istatus
            join $ atomicModifyIORef' istatus $ turnToAuto a tid
            return a
  where
    increment istatus = do
        status <- readIORef istatus
        increment' status
        where
            increment' (AutoUpdated a counter tid) = do
                cnt <- optimisticIncrement counter
                return $ Return a
            increment' (ManualUpdates counter) = do
                cnt <- optimisticIncrement counter
                return $ if cnt > updateSpawnThreshold then Spawn else Manual

            optimisticIncrement :: IORef Int -> IO Int
            optimisticIncrement counter = do
                cnt <- readIORef counter
                let inc = succ cnt
                writeIORef counter inc
                return inc

    -- Normal case.
    turnToAuto a tid (ManualUpdates cnt)     = (AutoUpdated a cnt tid
                                               ,return ())
    -- Race condition: multiple threads were spawned.
    -- So, let's kill the previous one by this thread.
    turnToAuto a tid (AutoUpdated _ cnt old) = (AutoUpdated a cnt tid
                                               ,throwTo old Replaced)

spawn :: UpdateSettings a -> IORef (Status a) -> IO ()
spawn UpdateSettings{..} istatus = handle (onErr istatus) $ forever $ do
    threadDelay updateFreq
    a <- updateAction
    ea <- checkActivity istatus
    case ea of
        Return msg -> error msg
        Manual -> stop -- turnToManual
        Spawn -> return () -- keep running
  where
    checkActivity :: IORef (Status a) -> IO (Action String)
    checkActivity istatus = do
        status <- readIORef istatus
        checkActivity' status where
            -- Normal case.
            checkActivity' (AutoUpdated _ counter tid) = do
                cnt <- readIORef counter
                writeIORef counter 0
                return $ if cnt >= 1 then Spawn else Manual
            -- This case must not happen.
            checkActivity' _ = return $ Return "Corrupt Status"

onErr :: IORef (Status a) -> SomeException -> IO ()
onErr istatus ex = case fromException ex of
    Just Replaced -> return () -- this thread is terminated
    Nothing -> do
        tid <- myThreadId
        counter <- newIORef 0
        atomicModifyIORef' istatus $ clear tid counter
        throwIO ex
  where
    -- In the race condition described above,
    -- suppose thread A is running, and is killed by thread B.
    -- Thread B then updates the IORef to refer to thread B.
    -- Then thread A's exception handler fires.
    -- We don't want to modify the IORef at all,
    -- since it refers to thread B already.
    -- Solution: only switch back to manual updates
    -- if the IORef is pointing at the current thread.
    clear tid counter (AutoUpdated _ _ tid') | tid == tid' = (ManualUpdates counter, ())
    clear _ _ status                                       = (status, ())

-- | Throw an error to kill a thread.
stop :: IO a
stop = throwIO Replaced
