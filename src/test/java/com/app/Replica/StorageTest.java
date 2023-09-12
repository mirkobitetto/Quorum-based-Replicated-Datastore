package com.app.Replica;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This class contains unit tests for the {@link com.app.Replica.Storage} class.
 * It tests various storage operations including acquiring and releasing write
 * locks,
 * getting values, putting values, and handling concurrent read and write locks.
 * The tests verify that the storage behaves correctly under different
 * scenarios.
 */

public class StorageTest {
    private Storage storage;

    @BeforeEach
    public void setUp() {
        storage = new Storage();
    }

    /**
     * Test acquiring and releasing write locks successfully.
     * This test checks if a client can successfully acquire a write lock on a key
     * and then release it, ensuring the lock is correctly released.
     */

    @Test
    public void testAcquireWriteLock_Success() {
        // Arrange
        String key = "testKey";
        InetSocketAddress holder = new InetSocketAddress("127.0.0.1", 12345);

        // Act
        Storage.WriteLockResult result = storage.acquireWriteLock(key, holder);

        // Assert
        assertTrue(result.isSuccess());
    }

    /**
     * Test acquiring a write lock when it's already acquired by another client.
     * This test simulates two clients trying to acquire a write lock on the same
     * key simultaneously. It verifies that one of them succeeds while the other
     * fails to acquire the lock.
     */

    @Test
    public void testAcquireWriteLock_Failure() {
        // Arrange
        String key = "testKey";
        InetSocketAddress holder1 = new InetSocketAddress("127.0.0.1", 12345);
        InetSocketAddress holder2 = new InetSocketAddress("127.0.0.1", 54321);

        // Act
        storage.acquireWriteLock(key, holder1);
        Storage.WriteLockResult result = storage.acquireWriteLock(key, holder2);

        // Assert
        assertFalse(result.isSuccess());
    }

    @Test
    public void testReleaseWriteLock_Success() {
        // Arrange
        String key = "testKey";
        InetSocketAddress holder = new InetSocketAddress("127.0.0.1", 12345);

        // Act
        storage.acquireWriteLock(key, holder);
        storage.releaseWriteLock(key, holder);

        // Assert
        // The lock should be released successfully, no exceptions expected
    }

    @Test
    public void testReleaseWriteLock_WrongHolder() {
        // Arrange
        String key = "testKey";
        InetSocketAddress holder1 = new InetSocketAddress("127.0.0.1", 12345);
        InetSocketAddress holder2 = new InetSocketAddress("127.0.0.1", 54321);

        // Act
        storage.acquireWriteLock(key, holder1);
        storage.releaseWriteLock(key, holder2);

        // Assert
        // The lock should not be released because the holder does not match
        assertTrue(storage.getWriteLockHolders().containsKey(key));
    }

    @Test
    public void testGet_Success() {
        // Arrange
        String key = "testKey";
        String value = "testValue";

        // Act
        storage.put(key, value, new InetSocketAddress("127.0.0.1", 12345), 1);
        String result = storage.get(key);

        // Assert
        assertEquals(value + " 1", result);
    }

    @Test
    public void testGet_LockNotAcquired() {
        // Arrange
        String key = "testKey";

        // Act
        String result = storage.get(key);

        // Assert
        assertNull(result);
    }

    @Test
    public void testPut_Success() {
        // Arrange
        String key = "testKey";
        String value = "testValue";
        InetSocketAddress holder = new InetSocketAddress("127.0.0.1", 12345);
        int version = 1;

        // Act
        boolean result = storage.put(key, value, holder, version);

        // Assert
        assertTrue(result);
        assertEquals(value, storage.getStore().get(key));
        assertEquals(version, storage.getVersions().get(key));
        assertFalse(storage.getWriteLockHolders().containsKey(key));
    }

    @Test
    public void testPut_WrongHolder() {
        // Arrange
        String key = "testKey";
        String value = "testValue";
        InetSocketAddress holder1 = new InetSocketAddress("127.0.0.1", 12345);
        InetSocketAddress holder2 = new InetSocketAddress("127.0.0.1", 54321);
        int version = 1;

        // Act
        storage.acquireWriteLock(key, holder1);
        boolean result = storage.put(key, value, holder2, version);

        // Assert
        assertFalse(result);
        assertTrue(storage.getWriteLockHolders().containsKey(key));
        assertNull(storage.getStore().get(key));
        assertEquals(0, storage.getVersions().get(key));
    }

    @Test
    public void testConcurrentWriteLocks() throws InterruptedException {
        // Arrange
        String key = "testKey";
        InetSocketAddress holder1 = new InetSocketAddress("127.0.0.1", 12345);
        InetSocketAddress holder2 = new InetSocketAddress("127.0.0.1", 54321);
        int version = 1;

        // Act
        Thread thread1 = new Thread(() -> {
            storage.acquireWriteLock(key, holder1);
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            storage.releaseWriteLock(key, holder1);
        });

        Thread thread2 = new Thread(() -> {
            storage.acquireWriteLock(key, holder2);
            storage.put(key, "testValue", holder2, version);
            storage.releaseWriteLock(key, holder2);
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // Assert
        assertTrue(storage.getWriteLockHolders().isEmpty());
        assertEquals("testValue", storage.getStore().get(key));
        assertEquals(version, storage.getVersions().get(key));
    }

    @Test
    public void testConcurrentReadLocks() throws InterruptedException {
        // Arrange
        String key = "testKey";

        // Act
        Thread thread1 = new Thread(() -> {
            storage.acquireWriteLock(key, new InetSocketAddress("127.0.0.1", 12345));
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            storage.releaseWriteLock(key, new InetSocketAddress("127.0.0.1", 12345));
        });

        Thread thread2 = new Thread(() -> {
            storage.acquireWriteLock(key, new InetSocketAddress("127.0.0.1", 54321));
            storage.get(key);
            storage.releaseWriteLock(key, new InetSocketAddress("127.0.0.1", 54321));
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // Assert
        assertTrue(storage.getWriteLockHolders().isEmpty());
        assertNull(storage.getStore().get(key));
    }
}
