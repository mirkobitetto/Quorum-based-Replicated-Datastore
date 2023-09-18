package com.app.Replica;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class StorageHandler implements Runnable {
    private Socket clientSocket;
    private Storage replica;

    public StorageHandler(Socket clientSocket, Storage replica) {
        this.clientSocket = clientSocket;
        this.replica = replica;
    }

    @Override
    public void run() {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);

            String request;
            while ((request = input.readLine()) != null) {
                // Process the request and call methods in the Replica class
                String[] parts = request.split(" ");
                String method = parts[2];
                String response = "";

                if ("ACQUIRE_LOCK".equals(method) && parts.length == 4) {
                    System.out.println("Acquiring write lock for key: " + parts[3]);
                    String key = parts[3];
                    InetSocketAddress clientAddress = new InetSocketAddress(
                            clientSocket.getInetAddress().getHostAddress(), clientSocket.getPort());
                    Storage.WriteLockResult lockResult = replica.acquireWriteLock(key, clientAddress);
                    response = lockResult.isSuccess() ? "LOCK_ACQUIRED " + lockResult.getCurrentVersion()
                            : "LOCK_NOT_ACQUIRED";
                } else if ("RELEASE_LOCK".equals(method) && parts.length == 4) {
                    System.out.println("Releasing lock for key " + parts[3]);
                    String key = parts[1];
                    InetSocketAddress clientAddress = new InetSocketAddress(
                            clientSocket.getInetAddress().getHostAddress(), clientSocket.getPort());
                    replica.releaseWriteLock(key, clientAddress);
                    response = "LOCK_RELEASED";
                } else if ("GET".equals(method) && parts.length == 4) {
                    System.out.println("Getting value for key " + parts[3]);
                    String key = parts[3];
                    String value = replica.get(key);
                    response = (value != null) ? "GET_SUCCESS " + value : "GET_FAILED";
                } else if ("PUT".equals(method) && parts.length == 6) {
                    System.out.println("Putting value for key " + parts[3]);
                    String key = parts[3];
                    String value = parts[4];
                    Integer version = Integer.parseInt(parts[5]);
                    InetSocketAddress clientAddress = new InetSocketAddress(
                            clientSocket.getInetAddress().getHostAddress(), clientSocket.getPort());
                    boolean success = replica.put(key, value, clientAddress, version);
                    response = success ? "PUT_SUCCESS" : "PUT_FAILED";
                } else if ("UPDATE".equals(method) && parts.length == 6) {
                    // Handle the "UPDATE" request
                    System.out.println("Trying to update stale response for key: " + parts[3]);
                    String key = parts[3];
                    String value = parts[4];
                    int receivedVersion = Integer.parseInt(parts[5]);

                    // Call the new update method in the replica
                    replica.update(key, value, receivedVersion);
                    response = "UPDATE_RECEIVED";
                } else {
                    response = "INVALID_REQUEST";
                }

                // Send the response back to the client
                output.println(response);
            }

            // Close the connection after handling all requests
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
