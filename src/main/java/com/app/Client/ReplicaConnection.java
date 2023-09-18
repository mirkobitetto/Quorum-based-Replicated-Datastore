package com.app.Client;

import java.io.*;
import java.net.Socket;

public class ReplicaConnection {
    private Socket clientSocket;
    private BufferedReader input;
    private PrintWriter output;
    private String clientID;

    public ReplicaConnection(String serverAddress, int serverPort, String clientID) throws IOException {
        this.clientID = clientID;
        try {
            clientSocket = new Socket(serverAddress, serverPort);
            input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            output = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            System.err.println("Client " + clientID + ": Failed to connect to the server. Please check that the replica is running.");
            System.exit(1);
        }
    }

    public void closeConnection() throws IOException {
        clientSocket.close();
    }

    public int acquireLock(String key) throws IOException {
        String acquireLockRequest = "Client " + clientID + ": ACQUIRE_LOCK " + key;
        output.println(acquireLockRequest);

        String lockResponse = input.readLine();

        if (lockResponse.startsWith("LOCK_ACQUIRED")) {
            String[] parts = lockResponse.split(" ");
            if (parts.length == 2) {
                return Integer.parseInt(parts[1]); // Return the version number
            }
        }

        return -1; // Return a default value (e.g., -1) to indicate an error or no version number
    }

    public boolean releaseLock(String key) throws IOException {
        String releaseLockRequest = "Client " + clientID + ": RELEASE_LOCK " + key;
        output.println(releaseLockRequest);
        String releaseLockResponse = input.readLine();
        return releaseLockResponse.equals("LOCK_RELEASED");
    }

    public String get(String key) throws IOException {
        String getRequest = "Client " + clientID + ": GET " + key;
        output.println(getRequest);
        return input.readLine();
    }

    public boolean put(String key, String value, int version) throws IOException {
        String putRequest = "Client " + clientID + ": PUT " + key + " " + value + " " + version;
        output.println(putRequest);
        String putResponse = input.readLine();
        return putResponse.equals("PUT_SUCCESS");
    }

    public boolean update(String key, String value, int version) throws IOException {
        String updateRequest = "Client " + clientID + ": UPDATE " + key + " " + value + " " + version;
        output.println(updateRequest);
        String updateResponse = input.readLine();
        return updateResponse.equals("UPDATE_RECEIVED");
    }

    public String toString() {
        return clientSocket.getLocalAddress().toString() + ":" + clientSocket.getPort();
    }
}