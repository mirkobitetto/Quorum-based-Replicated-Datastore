package com.app.Replica;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Storage {
    // Data structure to store key-value pairs
    private ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    // Data structure to store version numbers for keys
    private ConcurrentHashMap<String, Integer> versions = new ConcurrentHashMap<>();

    // Data structure to store write lock holders
    private ConcurrentHashMap<String, InetSocketAddress> writeLockHolders = new ConcurrentHashMap<>();

    // Data structure to store read-write locks for keys
    private ConcurrentHashMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public WriteLockResult acquireWriteLock(String key, InetSocketAddress holder) {
        // Acquire the write lock on the key
        ReentrantReadWriteLock lock = locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());

        InetSocketAddress currentHolder = writeLockHolders.get(key);

        if (currentHolder == null) {
            // No lock on the key
            try {
                if (lock.writeLock().tryLock(0, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                    writeLockHolders.put(key, holder);
                    int currentVersion = versions.getOrDefault(key, 0);
                    return new WriteLockResult(true, currentVersion); // Lock acquired
                } else {
                    return new WriteLockResult(false, 0); // Lock not acquired
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (currentHolder.equals(holder)) {
            // Clients already holds the lock
            int currentVersion = versions.getOrDefault(key, 0);
            return new WriteLockResult(true, currentVersion);
        }

        return new WriteLockResult(false, 0);

    }

    public void releaseWriteLock(String key, InetSocketAddress holder) {
        // Release the write lock on the key if held by the specified holder
        ReentrantReadWriteLock lock = locks.get(key);
        if (lock != null) {
            try {
                InetSocketAddress currentHolder = writeLockHolders.get(key);
                if (currentHolder != null && currentHolder.equals(holder)) {
                    writeLockHolders.remove(key);
                    // Release the write lock on the key
                    lock.writeLock().unlock();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public String get(String key) {
        // Acquire read lock on the key
        ReentrantReadWriteLock lock = locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());

        // Try to acquire the read lock without blocking the thread
        boolean lockAcquired = lock.readLock().tryLock();

        if (!lockAcquired) {
            // Failed to acquire the lock, return null or handle the case as needed
            System.out.println("Failed to acquire read lock for key: " + key);
            return null;
        }

        try {
            String value = store.get(key);
            int version;

            if (value == null) {
                // Value not yet stored on this replica
                value = "";
                version = -1;
            } else {
                version = versions.get(key);
            }

            // Return a copy of the value with the current version number
            return new String(value + " " + version);
        } finally {
            // Release read lock on the key
            lock.readLock().unlock();
        }
    }

    public boolean put(String key, String value, InetSocketAddress holder, Integer version) {
        // Check if the client holds the write lock for the given key
        InetSocketAddress currentHolder = writeLockHolders.get(key);
        if (currentHolder == null || !currentHolder.equals(holder)) {
            // Client does not hold the write lock for the key
            return false;
        }
        try {
            store.put(key, value);
            versions.put(key, version);
            return true;
        } finally {
            // Release the write lock on the key
            releaseWriteLock(key, holder);
            // remove the holder from the write lock holders
            writeLockHolders.remove(key);
        }
    }

    public static class WriteLockResult {
        private boolean success;
        private int currentVersion;

        public WriteLockResult(boolean success, int currentVersion) {
            this.success = success;
            this.currentVersion = currentVersion;
        }

        public boolean isSuccess() {
            return success;
        }

        public int getCurrentVersion() {
            return currentVersion;
        }

        @Override
        public String toString() {
            return "WriteLockResult [success=" + success + ", currentVersion=" + currentVersion + "]";
        }
    }

    // Getters for testing
    public ConcurrentHashMap<String, String> getStore() {
        return store;
    }

    public ConcurrentHashMap<String, Integer> getVersions() {
        return versions;
    }

    public ConcurrentHashMap<String, InetSocketAddress> getWriteLockHolders() {
        return writeLockHolders;
    }
}
