package com.app.Replica;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class AntiEntropy {
    private Storage storage;
    private int intervalInSeconds;
    private List<InetSocketAddress> replicaAddresses;
    private int numReplicasToSendAntiEntropy; // Field to store the number of replicas to send anti-entropy

    public AntiEntropy(Storage storage, int intervalInSeconds) {
        this.storage = storage;
        this.intervalInSeconds = intervalInSeconds;
        this.replicaAddresses = new ArrayList<>();
        this.numReplicasToSendAntiEntropy = 0; // Initialize to 0, will be set from the config file
    }

    public void startAntiEntropy() {
        loadReplicaAddresses();
        loadNumReplicasToSendAntiEntropy(); // Load the value from the config file
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::performAntiEntropy, intervalInSeconds, intervalInSeconds, TimeUnit.SECONDS);
    }

    private void loadReplicaAddresses() {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (inputStream != null) {
                properties.load(inputStream);
                int numReplicas = Integer.parseInt(properties.getProperty("numReplicas"));

                for (int i = 1; i <= numReplicas; i++) {
                    String replicaKey = "replica" + i;
                    String replicaInfo = properties.getProperty(replicaKey);
                    if (replicaInfo != null) {
                        // Parse and add replica addresses
                        InetSocketAddress replicaAddress = parseReplicaAddress(replicaInfo);
                        replicaAddresses.add(replicaAddress);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadNumReplicasToSendAntiEntropy() {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (inputStream != null) {
                properties.load(inputStream);
                numReplicasToSendAntiEntropy = Integer.parseInt(properties.getProperty("numReplicasToSendAntiEntropy"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private InetSocketAddress parseReplicaAddress(String replicaInfo) {
        String[] parts = replicaInfo.split(":");
        if (parts.length == 2) {
            String ipAddress = parts[0];
            int port = Integer.parseInt(parts[1]);
            return new InetSocketAddress(ipAddress, port);
        }
        return null;
    }

    private void performAntiEntropy() {
        // Send anti-entropy messages to a subset of the replica addresses
        // Compare data and versions with other replicas and perform updates as needed

        String key = storage.getRandomKey();

        // if key is equal to null then return
        if (key == null) {
            return;
        }

        String request = storage.get(key);

        // if value is not currently stored in replica return
        if (request.equals(" -1")) {
            return;
        }

        // split the request into the value and version
        String[] parts = request.split(" ");
        String value = parts[0];
        int version = Integer.parseInt(parts[1]);

        // Shuffle the replica addresses to randomize the selection
        List<InetSocketAddress> shuffledAddresses = new ArrayList<>(replicaAddresses);
        for (int i = shuffledAddresses.size() - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(i + 1);
            InetSocketAddress temp = shuffledAddresses.get(index);
            shuffledAddresses.set(index, shuffledAddresses.get(i));
            shuffledAddresses.set(i, temp);
        }

        // Send anti-entropy messages to a subset of the replicas based on
        // numReplicasToSendAntiEntropy
        for (int i = 0; i < Math.min(numReplicasToSendAntiEntropy, shuffledAddresses.size()); i++) {
            InetSocketAddress replicaAddress = shuffledAddresses.get(i);
            if (!sendUpdateRequest(replicaAddress, key, value, version)) {
                // Handle the case where the update request failed
                System.out.println("Failed to send update request to replica: " + replicaAddress);
            }
        }
    }

    private boolean sendUpdateRequest(InetSocketAddress replicaAddress, String key, String value, int version) {
        try (Socket socket = new Socket(replicaAddress.getAddress(), replicaAddress.getPort());
                PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String updateRequest = "Replica "+ replicaAddress + ": UPDATE " + key + " " + value + " " + version;
            output.println(updateRequest);

            // Receive the response from the replica
            String updateResponse = input.readLine();

            // Handle the response as needed
            return updateResponse.equals("UPDATE_RECEIVED");

        } catch (IOException e) {
            System.out.println("Failed to send anti-entropy message to replica: " + replicaAddress);
        }

        // The update request failed
        return false;
    }
}
