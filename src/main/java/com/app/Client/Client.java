package com.app.Client;

import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        printAsciiArt();
        System.out.println("Welcome to the Quorum-based Replicated Datastore Client!\n");

        Quorum quorum = new Quorum();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\nChoose an operation:");
            System.out.println("1. Put (enter 'put')");
            System.out.println("2. Get (enter 'get')");
            System.out.println("3. Exit (enter 'exit')");
            System.out.print("Enter your choice: ");

            String choice = scanner.nextLine();

            if ("put".equalsIgnoreCase(choice)) {
                System.out.print("Enter key: ");
                String key = scanner.nextLine();
                System.out.print("Enter value: ");
                String value = scanner.nextLine();
                quorum.putValue(key, value);
            } else if ("get".equalsIgnoreCase(choice)) {
                System.out.print("Enter key: ");
                String key = scanner.nextLine();
                quorum.getValue(key);
            } else if ("exit".equalsIgnoreCase(choice)) {
                System.out.println("\nExiting the client. Goodbye!");
                break;
            } else {
                System.out.println("Invalid choice. Please enter 'put', 'get', or 'exit'.");
            }
        }

        scanner.close();
    }

    private static void printAsciiArt() {
        String asciiArt = " _____ _ _            _   \n" +
                "/  __ \\ (_)          | |  \n" +
                "| /  \\/ |_  ___ _ __ | |_ \n" +
                "| |   | | |/ _ \\ '_ \\| __|\n" +
                "| \\__/\\ | |  __/ | | | |_ \n" +
                " \\____/_|_|\\___|_| |_|\\__|\n" +
                "                          \n" +
                "                          ";

        System.out.println(asciiArt);
    }
}
