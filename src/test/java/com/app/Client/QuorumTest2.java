package com.app.Client;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class contains unit tests for the {@link com.app.Client.Quorum} class.
 * These tests focus on client operations (PUT and GET) when multiple clients
 * are involved, ensuring that functionality works correctly with concurrency.
 */

public class QuorumTest2 {

    private int numClients = 2; // Adjust this as needed
    private int numOperations = 10; // Adjust this as needed
    private CountDownLatch startSignal = new CountDownLatch(1);
    private CountDownLatch doneSignal = new CountDownLatch(numClients);

    @Before
    public void setUp() {
        // Initialize the Quorum instances for each client here if needed.
    }

    @Test
    public void testMultipleGetOnDifferentKeys() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numClients);

        for (int i = 0; i < numClients; i++) {
            final int clientNr = i;
            executorService.execute(() -> {
                try {
                    String clientID = Integer.toString(clientNr + 1);
                    Quorum quorum = new Quorum(clientID); // Create a Quorum instance for each client
                    startSignal.await(); // Wait for the signal to start concurrently
                    performConcurrentGetOnDifferentKeys(quorum, numOperations, clientNr); // Perform concurrent
                                                                                          // operations
                    doneSignal.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startSignal.countDown(); // Release the start signal to start all clients concurrently
        doneSignal.await(); // Wait for all clients to finish

    }

    @Test
    public void testMultipleGetOnSameKey() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numClients);

        for (int i = 0; i < numClients; i++) {
            final int clientNr = i;
            executorService.execute(() -> {
                try {
                    String clientID = Integer.toString(clientNr + 1);
                    Quorum quorum = new Quorum(clientID); // Create a Quorum instance for each client
                    startSignal.await(); // Wait for the signal to start concurrently
                    performConcurrentGetOnSameKey(quorum, numOperations); // Perform concurrent operations
                    doneSignal.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startSignal.countDown(); // Release the start signal to start all clients concurrently
        doneSignal.await(); // Wait for all clients to finish

    }

    @Test
    public void testGetWhileWriteInProgress() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numClients);

        for (int i = 0; i < numClients; i++) {
            final int clientNr = i;
            executorService.execute(() -> {
                try {
                    String clientID = Integer.toString(clientNr + 1);
                    Quorum quorum = new Quorum(clientID); // Create a Quorum instance for each client
                    startSignal.await(); // Wait for the signal to start concurrently
                    performConcurrentGetWhileWriteInProgress(quorum, numOperations, clientNr); // Perform
                                                                                               // concurrent
                                                                                               // operations
                    doneSignal.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startSignal.countDown(); // Release the start signal to start all clients concurrently
        doneSignal.await(); // Wait for all clients to finish

    }

    @Test
    public void testMultipleWritesOnDifferentKeys() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(numClients);

        for (int i = 0; i < numClients; i++) {
            final int clientNr = i;
            executorService.execute(() -> {
                try {
                    String clientID = Integer.toString(clientNr + 1);
                    Quorum quorum = new Quorum(clientID); // Create a Quorum instance for each client
                    startSignal.await(); // Wait for the signal to start concurrently
                    performConcurrentWritesOnDifferentKeys(quorum, numOperations); // Perform concurrent operations
                    doneSignal.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startSignal.countDown(); // Release the start signal to start all clients concurrently
        doneSignal.await(); // Wait for all clients to finish

    }

    private void performConcurrentGetOnDifferentKeys(Quorum quorum, int numOperations, int clientNr) {
        // Test case 1: Multiple GET operations on different keys
        // Simulate concurrent GET operations on different keys and assert the behavior

        // Initialize the Quorum instance with some values

        quorum.putValue("test1key" + clientNr, "test1value" + clientNr);

        // Simulate concurrent GET operations on different keys and assert the behavior

        for (int i = 0; i < numOperations; i++) {

            String resp = quorum.getValue("test1key" + clientNr);

            String value;
            if (resp == null) {
                value = null;
            } else {
                value = resp.split(" ")[1];
            }

            assertEquals("test1value" + clientNr, value);
        }

    }

    private void performConcurrentGetOnSameKey(Quorum quorum, int numOperations) {
        // Implement test case 2: Multiple GET operations on the same key
        // Simulate concurrent GET operations on the same key and assert the behavior

        // Initialize the Quorum instance with some values

        quorum.putValue("test2key", "test2value");

        for (int i = 0; i < numOperations; i++) {

            String resp = quorum.getValue("test2key");

            String value;
            if (resp == null) {
                value = null;
            } else {
                value = resp.split(" ")[1];
            }

            assertEquals("test2value", value);
        }
    }

    private void performConcurrentGetWhileWriteInProgress(Quorum quorum, int numOperations, int writeQuorumIndex) {
        // Implement test case 3: GET operation while write is in progress on the same
        // key
        // Simulate concurrent GET operations while a write is in progress on the same
        // key and assert the behavior

        // Initialize a CountDownLatch for the writing quorum to start writing
        CountDownLatch writeStartLatch = new CountDownLatch(1);

        // Determine whether this quorum should write or read
        boolean isWriteQuorum = (writeQuorumIndex == 0);

        if (isWriteQuorum) {
            // Start a thread to perform a write operation
            Thread writeThread = new Thread(() -> {
                try {
                    writeStartLatch.await(); // Wait for the signal to start writing
                    quorum.putValue("test3key", "writeInProgressValue");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
        }

        // Simulate concurrent GET operations

        if (isWriteQuorum) {
            // Release the writeStartLatch only for the write quorum
            writeStartLatch.countDown();
        }

        // Perform a GET operation while the write is in progress
        String resp = quorum.getValue("test3key");

        String value;
        if (resp == null) {
            value = null;
        } else {
            value = resp.split(" ")[1];
        }

        // Assert that the retrieved value matches null since the get should fail
        assertEquals(null, value);

    }

    private void performConcurrentWritesOnDifferentKeys(Quorum quorum, int numOperations) {
        // Implement test case 4: Multiple WRITE operations on different keys
        // Simulate concurrent WRITE operations on different keys and assert the
        // behavior

        // Create an array of keys
        String[] keys = new String[numOperations];

        for (int i = 0; i < numOperations; i++) {
            keys[i] = "test4key" + i;
        }

        // Create a CountDownLatch to coordinate concurrent writes
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < numOperations; i++) {
            String key = keys[i];
            String newValue = "newValue" + i;

            // Start a thread for each concurrent write operation
            Thread writeThread = new Thread(() -> {
                try {
                    startLatch.await(); // Wait for the signal to start writing concurrently

                    // Use synchronized block to ensure thread safety while updating the key-value
                    // store
                    synchronized (quorum) {
                        // Perform concurrent WRITE operation on different keys
                        quorum.putValue(key, newValue);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            writeThread.start();
        }

        // Release the startLatch to initiate concurrent writes
        startLatch.countDown();

        // Wait for all concurrent write threads to finish
        try {
            Thread.sleep(100); // Adjust sleep time as needed to ensure all threads finish
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Retrieve the final values for the keys and assert that they match the latest
        // written values
        for (int i = 0; i < numOperations; i++) {
            String key = keys[i];
            String expectedValue = "newValue" + i;

            String retrievedValue;
            synchronized (quorum) {
                retrievedValue = quorum.getValue(key);
            }

            String[] parts = retrievedValue.split(" ");
            retrievedValue = parts[1];

            // Assert that the final value matches the latest written value
            assertEquals(expectedValue, retrievedValue);
        }
    }
}
