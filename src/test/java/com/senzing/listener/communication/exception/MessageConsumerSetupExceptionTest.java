package com.senzing.listener.communication.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MessageConsumerSetupException}.
 */
class MessageConsumerSetupExceptionTest {

    private static final String TEST_MESSAGE = "Test setup error message";

    @Test
    void testConstructWithMessage() {
        MessageConsumerSetupException exception = new MessageConsumerSetupException(TEST_MESSAGE);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithCause() {
        Exception cause = new RuntimeException("Underlying cause");

        MessageConsumerSetupException exception = new MessageConsumerSetupException(cause);

        assertSame(cause, exception.getCause());
        assertTrue(exception.getMessage().contains(cause.toString()));
    }

    @Test
    void testConstructWithMessageAndCause() {
        Exception cause = new RuntimeException("Underlying cause");

        MessageConsumerSetupException exception = new MessageConsumerSetupException(TEST_MESSAGE, cause);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testConstructWithNullMessage() {
        MessageConsumerSetupException exception = new MessageConsumerSetupException((String) null);

        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithNullCause() {
        MessageConsumerSetupException exception = new MessageConsumerSetupException((Exception) null);

        assertNull(exception.getCause());
    }

    @Test
    void testConstructWithMessageAndNullCause() {
        MessageConsumerSetupException exception = new MessageConsumerSetupException(TEST_MESSAGE, null);

        assertEquals(TEST_MESSAGE, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testExtendsMessageConsumerException() {
        MessageConsumerSetupException exception = new MessageConsumerSetupException(TEST_MESSAGE);

        assertTrue(exception instanceof MessageConsumerException);
    }

    @Test
    void testIsException() {
        MessageConsumerSetupException exception = new MessageConsumerSetupException(TEST_MESSAGE);

        assertTrue(exception instanceof Exception);
    }
}
