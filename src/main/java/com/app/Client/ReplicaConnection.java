package com.app.Client;

import java.io.*;
import java.net.Socket;

public class ReplicaConnection {
    private Socket clientSocket;
    private BufferedReader input;
    private PrintWriter output;

    public ReplicaConnection(String serverAddress, int serverPort) throws IOException {
        try {
            clientSocket = new Socket(serverAddress, serverPort);
            input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            output = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            System.err.println("Failed to connect to the server. Please check that the replica is running.");
            System.exit(1);
        }
    }

    public void closeConnection() throws IOException {
        clientSocket.close();
    }

    public int acquireLock(String key) throws IOException {
        String acquireLockRequest = "ACQUIRE_LOCK " + key;
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
        String releaseLockRequest = "RELEASE_LOCK " + key;
        output.println(releaseLockRequest);
        String releaseLockResponse = input.readLine();
        return releaseLockResponse.equals("LOCK_RELEASED");
    }

    public String get(String key) throws IOException {
        String getRequest = "GET " + key;
        output.println(getRequest);
        return input.readLine();
    }

    public boolean put(String key, String value, int version) throws IOException {
        String putRequest = "PUT " + key + " " + value + " " + version;
        output.println(putRequest);
        String putResponse = input.readLine();
        return putResponse.equals("PUT_SUCCESS");
    }

    public String toString() {
        return clientSocket.getLocalAddress().toString() + ":" + clientSocket.getPort();
    }
}