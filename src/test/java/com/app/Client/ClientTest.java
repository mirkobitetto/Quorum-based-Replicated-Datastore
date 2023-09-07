package com.app.Client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * This class contains unit tests for the {@link com.app.Client.Client} class.
 * It tests various client operations including PUT, GET, and invalid
 * operations.
 * The tests verify that the client produces the expected output based on
 * different input scenarios.
 */

public class ClientTest {
    private final InputStream originalSystemIn = System.in;
    private final PrintStream originalSystemOut = System.out;
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    @BeforeEach
    public void setUpStreams() {
        System.setOut(new PrintStream(outputStream));
    }

    @AfterEach
    public void restoreStreams() {
        System.setIn(originalSystemIn);
        System.setOut(originalSystemOut);
    }

    @Test
    public void testPutOperation() {
        String input = "put\nkey1\nvalue1\nexit\n";
        System.setIn(new ByteArrayInputStream(input.getBytes()));

        Client.main(new String[] {});

        String output = outputStream.toString();
        // Verify that the output contains the expected messages after the "put"
        // operation.
        assertEquals(true, output.contains("PUT operation successful"));
        assertEquals(true, output.contains("Exiting the client. Goodbye!"));
    }

    @Test
    public void testGetOperation() {
        String input = "get\nkey2\nexit\n";
        System.setIn(new ByteArrayInputStream(input.getBytes()));

        Client.main(new String[] {});

        String output = outputStream.toString();
        // Verify that the output contains the expected messages after the "get"
        // operation.
        assertEquals(true, output.contains("GET operation successful"));
        assertEquals(true, output.contains("Exiting the client. Goodbye!"));
    }

    @Test
    public void testInvalidOperation() {
        String input = "invalid\nexit\n";
        System.setIn(new ByteArrayInputStream(input.getBytes()));

        Client.main(new String[] {});

        String output = outputStream.toString();
        // Verify that the output contains the error message for an invalid operation.
        assertEquals(true, output.contains("Invalid choice. Please enter 'put', 'get', or 'exit'."));
        assertEquals(true, output.contains("Exiting the client. Goodbye!"));
    }
}
