package com.senzing.listener.communication.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MessageConsumerException}.
 */
class MessageConsumerExceptionTest {

    private static final String TEST_MESSAGE = "Test error message";

    @Test
    void testConstructWithMessage() {
        MessageConsumerException exception = new MessageConsumerException(TEST_MESSAGE);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithCause() {
        Exception cause = new RuntimeException("Underlying cause");

        MessageConsumerException exception = new MessageConsumerException(cause);

        assertSame(cause, exception.getCause());
        assertTrue(exception.getMessage().contains(cause.toString()));
    }

    @Test
    void testConstructWithMessageAndCause() {
        Exception cause = new RuntimeException("Underlying cause");

        MessageConsumerException exception = new MessageConsumerException(TEST_MESSAGE, cause);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testConstructWithNullMessage() {
        MessageConsumerException exception = new MessageConsumerException((String) null);

        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithNullCause() {
        MessageConsumerException exception = new MessageConsumerException((Exception) null);

        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithMessageAndNullCause() {
        MessageConsumerException exception = new MessageConsumerException(TEST_MESSAGE, null);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testIsException() {
        MessageConsumerException exception = new MessageConsumerException(TEST_MESSAGE);

        assertTrue(exception instanceof Exception);
    }
}
