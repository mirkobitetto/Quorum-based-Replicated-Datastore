package com.app.Replica;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

public class Replica {
    public static void main(String[] args) {

        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
        final Logger logger = LogManager.getLogger(Replica.class);

        if (args.length != 1) {
            logger.info("Usage: java Replica <serverPort>");
            return;
        }

        int serverPort;
        int antiEntropyIntervalInSeconds;

        try {
            serverPort = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            logger.error("Invalid server port. Please provide a valid integer.");
            return;
        }

        ThreadContext.put("port", String.valueOf(serverPort));

        try {
            Properties properties = loadConfigFile("config.properties");
            if (properties == null) {
                logger.error("Error: Failed to load configuration file.");
                return;
            }

            antiEntropyIntervalInSeconds = Integer.parseInt(properties.getProperty("antiEntropyIntervalInSeconds"));
            // Load replica IP addresses
            List<String> replicaIPs = loadReplicaIPs(properties);

            Storage replica = new Storage();

            // Start anti-entropy
            AntiEntropy antiEntropy = new AntiEntropy(replica, antiEntropyIntervalInSeconds);
            antiEntropy.startAntiEntropy();

            try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
                logger.info("Replica listening on port " + serverPort);

                while (true) {
                    // Wait for a client connection
                    Socket clientSocket = serverSocket.accept();

                    if (isReplicaConnection(clientSocket, replicaIPs)) {
                        // This is a replica connection
                        logger.info("Replica connected. Replica IP: " + clientSocket.getInetAddress().getHostAddress()
                                + " Port: " + clientSocket.getPort());
                    } else {
                        // This is a client connection
                        logger.info("Client connected. Client IP: " + clientSocket.getInetAddress().getHostAddress()
                                + " Port: " + clientSocket.getPort());
                    }

                    // Create a new thread to handle the client request
                    StorageHandler requestHandler = new StorageHandler(clientSocket, replica);
                    Thread thread = new Thread(requestHandler);
                    thread.start();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Load the configuration file from the resources folder
    private static Properties loadConfigFile(String configFile) {
        Properties properties = new Properties();
        try (InputStream inputStream = Replica.class.getClassLoader().getResourceAsStream(configFile)) {
            if (inputStream != null) {
                properties.load(inputStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    private static List<String> loadReplicaIPs(Properties properties) {
        List<String> replicaIPs = new ArrayList<>();
        int numReplicas = Integer.parseInt(properties.getProperty("numReplicas"));

        for (int i = 1; i <= numReplicas; i++) {
            String replicaKey = "replica" + i;
            String replicaInfo = properties.getProperty(replicaKey);
            if (replicaInfo != null) {
                String[] parts = replicaInfo.split(":");
                if (parts.length == 2) {
                    replicaIPs.add(parts[0]); // Add the IP address to the list
                }
            }
        }
        return replicaIPs;
    }

    // Check if a connection is from a replica
    private static boolean isReplicaConnection(Socket socket, List<String> replicaIPs) {
        String clientIP = socket.getInetAddress().getHostAddress();
        return replicaIPs.contains(clientIP);
    }
}
