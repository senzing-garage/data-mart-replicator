package com.senzing.listener.service.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ServiceSetupException}.
 */
class ServiceSetupExceptionTest {

    private static final String TEST_MESSAGE = "Test setup error message";

    @Test
    void testConstructWithMessage() {
        ServiceSetupException exception = new ServiceSetupException(TEST_MESSAGE);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithCause() {
        Exception cause = new RuntimeException("Underlying cause");

        ServiceSetupException exception = new ServiceSetupException(cause);

        assertSame(cause, exception.getCause());
        assertTrue(exception.getMessage().contains(cause.toString()));
    }

    @Test
    void testConstructWithMessageAndCause() {
        Exception cause = new RuntimeException("Underlying cause");

        ServiceSetupException exception = new ServiceSetupException(TEST_MESSAGE, cause);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testConstructWithNullMessage() {
        ServiceSetupException exception = new ServiceSetupException((String) null);

        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithNullCause() {
        ServiceSetupException exception = new ServiceSetupException((Exception) null);

        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithMessageAndNullCause() {
        ServiceSetupException exception = new ServiceSetupException(TEST_MESSAGE, null);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testIsException() {
        ServiceSetupException exception = new ServiceSetupException(TEST_MESSAGE);

        assertTrue(exception instanceof Exception);
    }
}
