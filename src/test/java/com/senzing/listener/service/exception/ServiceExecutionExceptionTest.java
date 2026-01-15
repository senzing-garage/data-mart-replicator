package com.senzing.listener.service.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ServiceExecutionException}.
 */
class ServiceExecutionExceptionTest {

    private static final String TEST_MESSAGE = "Test execution error message";

    @Test
    void testConstructWithMessage() {
        ServiceExecutionException exception = new ServiceExecutionException(TEST_MESSAGE);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithCause() {
        Exception cause = new RuntimeException("Underlying cause");

        ServiceExecutionException exception = new ServiceExecutionException(cause);

        assertSame(cause, exception.getCause());
        assertTrue(exception.getMessage().contains(cause.toString()));
    }

    @Test
    void testConstructWithMessageAndCause() {
        Exception cause = new RuntimeException("Underlying cause");

        ServiceExecutionException exception = new ServiceExecutionException(TEST_MESSAGE, cause);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testConstructWithNullMessage() {
        ServiceExecutionException exception = new ServiceExecutionException((String) null);

        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithNullCause() {
        ServiceExecutionException exception = new ServiceExecutionException((Exception) null);

        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithMessageAndNullCause() {
        ServiceExecutionException exception = new ServiceExecutionException(TEST_MESSAGE, null);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testIsException() {
        ServiceExecutionException exception = new ServiceExecutionException(TEST_MESSAGE);

        assertTrue(exception instanceof Exception);
    }
}
