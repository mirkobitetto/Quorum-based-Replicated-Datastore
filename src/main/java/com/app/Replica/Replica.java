package com.app.Replica;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Replica {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Replica <serverPort>");
            return;
        }

        int serverPort;
        try {
            serverPort = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid server port. Please provide a valid integer.");
            return;
        }

        try {
            Storage replica = new Storage();

            ServerSocket serverSocket = new ServerSocket(serverPort);
            System.out.println("Replica listening on port " + serverPort);

            while (true) {
                // Wait for a client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected. Client IP: " + clientSocket.getInetAddress().getHostAddress()
                        + " Port: " + clientSocket.getPort());

                // Create a new thread to handle the client request
                StorageHandler requestHandler = new StorageHandler(clientSocket, replica);
                Thread thread = new Thread(requestHandler);
                thread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
