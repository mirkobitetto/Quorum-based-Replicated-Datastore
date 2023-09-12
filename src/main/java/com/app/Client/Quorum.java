package com.app.Client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Quorum {
    private List<String> readQuorumList;
    private List<String> writeQuorumList;

    public Quorum() {
        try {
            // Load the configuration file using the ClassLoader
            Properties properties = loadConfigFile("config.properties");

            if (properties == null) {
                System.out.println("Error: Failed to load configuration file.");
                return;
            }

            // Rest of your code remains unchanged
            int numReplicas = Integer.parseInt(properties.getProperty("numReplicas"));
            int readQuorum = Integer.parseInt(properties.getProperty("readQuorum"));
            int writeQuorum = Integer.parseInt(properties.getProperty("writeQuorum"));

            // Perform runtime checks on the read quorum and write quorum
            if (writeQuorum <= numReplicas / 2 || writeQuorum + readQuorum <= numReplicas) {
                System.out.println("Error: Invalid read and write quorum values. Please check the configuration file.");
                return;
            }

            // Create a List to hold connections to all replicas
            List<String> replicaInfoList = new ArrayList<>();
            for (int i = 1; i <= numReplicas; i++) {
                String replicaKey = "replica" + i;
                String replicaInfo = properties.getProperty(replicaKey);
                if (replicaInfo == null) {
                    System.out.println("Replica information not found for " + replicaKey);
                    return;
                }
                replicaInfoList.add(replicaInfo);
            }

            // Create Set to hold read quorum replicas (no duplicates)
            Set<String> readQuorumSet = new HashSet<>();
            while (readQuorumSet.size() < readQuorum) {
                int randomIndex = (int) (Math.random() * replicaInfoList.size());
                readQuorumSet.add(replicaInfoList.get(randomIndex));
            }
            readQuorumList = new ArrayList<>(readQuorumSet);

            // Create Set to hold write quorum replicas (no duplicates)
            Set<String> writeQuorumSet = new HashSet<>();
            while (writeQuorumSet.size() < writeQuorum) {
                int randomIndex = (int) (Math.random() * replicaInfoList.size());
                writeQuorumSet.add(replicaInfoList.get(randomIndex));
            }
            writeQuorumList = new ArrayList<>(writeQuorumSet);

            // Print the read quorum and write quorum
            System.out.println("Read quorum: " + readQuorumList);
            System.out.println("Write quorum: " + writeQuorumList);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Load the configuration file using the ClassLoader
    private Properties loadConfigFile(String configFile) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (inputStream != null) {
                properties.load(inputStream);
            }
        }
        return properties;
    }

    public List<String> getReadQuorumList() {
        return readQuorumList;
    }

    public List<String> getWriteQuorumList() {
        return writeQuorumList;
    }

    public int getNumReplicas() {
        return readQuorumList.size() + writeQuorumList.size();
    }

    public String getValue(String key) {
        // list to hold all values returned by replicas
        List<String> values = new ArrayList<>();
        for (String replicaInfo : readQuorumList) {
            String[] parts = replicaInfo.split(":");
            String serverAddress = parts[0];
            int serverPort = Integer.parseInt(parts[1]);

            try {
                ReplicaConnection connection = new ReplicaConnection(serverAddress, serverPort);
                String value = connection.get(key);
                values.add(value);
                System.out.println("GET operation successful on Replica " + connection.toString()
                        + ". Value: " + value);
                connection.closeConnection();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Find the value with the highest version number
        int highestVersionNumber = -1;
        String mostRecentValue = null;

        for (String value : values) {
            if (value.startsWith("GET_SUCCESS") && value != null) {
                System.out.println("Value: " + value);
                String[] parts = value.split(" ");
                int version = Integer.parseInt(parts[2]);

                // Update the most recent value if a higher version number is encountered
                if (version > highestVersionNumber) {
                    highestVersionNumber = version;
                    mostRecentValue = value;
                }
            }
        }

        System.out.println("GET RESULT: " + mostRecentValue);
        return mostRecentValue;

    }

    public String putValue(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key or value cannot be null");
        }
        // Create connections to replicas
        List<ReplicaConnection> replicaConnections = new ArrayList<>();
        for (String replicaInfo : writeQuorumList) {
            String[] parts = replicaInfo.split(":");
            String serverAddress = parts[0];
            int serverPort = Integer.parseInt(parts[1]);
            try {
                ReplicaConnection connection = new ReplicaConnection(serverAddress, serverPort);
                replicaConnections.add(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Acquire locks on all replicas
        List<Integer> versionNumbers = new ArrayList<>();
        for (ReplicaConnection connection : replicaConnections) {
            int lockAcquired = -1;
            try {
                lockAcquired = connection.acquireLock(key);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (lockAcquired == -1) {
                System.out.println("Failed to acquire lock on Replica: " + (connection.toString()));
                releaseLocks(replicaConnections, key);
                closeReplicaConnections(replicaConnections);
                return "false";
            } else {
                System.out.println("Lock acquired on Replica " + (connection.toString())
                        + " Version Number: " + lockAcquired);
                versionNumbers.add(lockAcquired);
            }
        }

        // Compute the new version number equal to the max of all version numbers + 1
        int newVersionNumber = Collections.max(versionNumbers) + 1;

        // Send PUT request to all replicas
        for (ReplicaConnection connection : replicaConnections) {
            try {
                if (connection.put(key, value, newVersionNumber)) {
                    System.out
                            .println("PUT operation successful on Replica " + (connection.toString())
                                    + ". Value: " + value + " Version Number: " + newVersionNumber);
                } else {
                    System.out.println("PUT operation failed on Replica " + (connection.toString()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Close connections to replicas
        closeReplicaConnections(replicaConnections);
        System.out.println("PUT operation successful");

        return "true";
    }

    // Helper method to close all connections to replicas
    private void closeReplicaConnections(List<ReplicaConnection> connections) {
        for (ReplicaConnection connection : connections) {
            try {
                connection.closeConnection();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Helper method to release locks on all replicas when a PUT operation fails
    private void releaseLocks(List<ReplicaConnection> connections, String key) {
        for (ReplicaConnection connection : connections) {
            try {
                connection.releaseLock(key);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
