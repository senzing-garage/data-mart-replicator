package com.senzing.datamart.reports;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for {@link ReportsServiceException}.
 * Tests all constructors and verifies proper message and cause handling.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ReportsServiceExceptionTest {

    /**
     * Test the default constructor (no arguments).
     * Verifies that exception can be created with null message and cause.
     */
    @Test
    @Order(100)
    void testDefaultConstructor() {
        ReportsServiceException exception = new ReportsServiceException();

        assertNotNull(exception, "Exception should not be null");
        assertNull(exception.getMessage(), "Message should be null");
        assertNull(exception.getCause(), "Cause should be null");
        assertTrue(exception instanceof RuntimeException,
            "Should be instance of RuntimeException");
    }

    /**
     * Test constructor with message only.
     * Verifies message is properly stored and cause is null.
     */
    @Test
    @Order(200)
    void testMessageConstructor() {
        String message = "Test exception message";
        ReportsServiceException exception = new ReportsServiceException(message);

        assertNotNull(exception, "Exception should not be null");
        assertEquals(message, exception.getMessage(),
            "Message should match");
        assertNull(exception.getCause(), "Cause should be null");
    }

    /**
     * Test constructor with null message.
     * Verifies null message is handled correctly.
     */
    @Test
    @Order(300)
    void testMessageConstructorWithNull() {
        ReportsServiceException exception = new ReportsServiceException((String) null);

        assertNotNull(exception, "Exception should not be null");
        assertNull(exception.getMessage(), "Message should be null");
        assertNull(exception.getCause(), "Cause should be null");
    }

    /**
     * Test constructor with empty message.
     * Verifies empty string is preserved.
     */
    @Test
    @Order(400)
    void testMessageConstructorWithEmptyString() {
        String message = "";
        ReportsServiceException exception = new ReportsServiceException(message);

        assertNotNull(exception, "Exception should not be null");
        assertEquals(message, exception.getMessage(),
            "Empty message should be preserved");
        assertNull(exception.getCause(), "Cause should be null");
    }

    /**
     * Test constructor with cause only.
     * Verifies cause is properly stored and message is derived from cause.
     */
    @Test
    @Order(500)
    void testCauseConstructor() {
        IOException cause = new IOException("IO error occurred");
        ReportsServiceException exception = new ReportsServiceException(cause);

        assertNotNull(exception, "Exception should not be null");
        assertSame(cause, exception.getCause(),
            "Cause should be the same instance");
        // When constructed with cause only, message is typically cause.toString()
        assertNotNull(exception.getMessage(), "Message should not be null");
        assertTrue(exception.getMessage().contains("IOException"),
            "Message should contain cause class name");
    }

    /**
     * Test constructor with null cause.
     * Verifies null cause is handled correctly.
     */
    @Test
    @Order(600)
    void testCauseConstructorWithNull() {
        ReportsServiceException exception = new ReportsServiceException((Throwable) null);

        assertNotNull(exception, "Exception should not be null");
        assertNull(exception.getCause(), "Cause should be null");
    }

    /**
     * Test constructor with different cause types.
     * Verifies various exception types can be used as cause.
     */
    @Test
    @Order(700)
    void testCauseConstructorWithDifferentTypes() {
        // Test with SQLException
        SQLException sqlCause = new SQLException("Database error", "42000");
        ReportsServiceException sqlException = new ReportsServiceException(sqlCause);
        assertSame(sqlCause, sqlException.getCause(),
            "SQLException cause should match");

        // Test with RuntimeException
        RuntimeException rtCause = new IllegalArgumentException("Invalid argument");
        ReportsServiceException rtException = new ReportsServiceException(rtCause);
        assertSame(rtCause, rtException.getCause(),
            "RuntimeException cause should match");

        // Test with Error
        Error errorCause = new AssertionError("Assertion failed");
        ReportsServiceException errorException = new ReportsServiceException(errorCause);
        assertSame(errorCause, errorException.getCause(),
            "Error cause should match");
    }

    /**
     * Test constructor with message and cause.
     * Verifies both message and cause are properly stored.
     */
    @Test
    @Order(800)
    void testMessageAndCauseConstructor() {
        String message = "Reports service failed";
        SQLException cause = new SQLException("Connection timeout");
        ReportsServiceException exception = new ReportsServiceException(message, cause);

        assertNotNull(exception, "Exception should not be null");
        assertEquals(message, exception.getMessage(),
            "Message should match");
        assertSame(cause, exception.getCause(),
            "Cause should be the same instance");
    }

    /**
     * Test constructor with message and null cause.
     * Verifies message is preserved when cause is null.
     */
    @Test
    @Order(900)
    void testMessageAndCauseConstructorWithNullCause() {
        String message = "Custom error message";
        ReportsServiceException exception = new ReportsServiceException(message, null);

        assertNotNull(exception, "Exception should not be null");
        assertEquals(message, exception.getMessage(),
            "Message should match");
        assertNull(exception.getCause(), "Cause should be null");
    }

    /**
     * Test constructor with null message and valid cause.
     * Verifies cause is preserved when message is null.
     */
    @Test
    @Order(1000)
    void testMessageAndCauseConstructorWithNullMessage() {
        IOException cause = new IOException("Network error");
        ReportsServiceException exception = new ReportsServiceException(null, cause);

        assertNotNull(exception, "Exception should not be null");
        assertNull(exception.getMessage(), "Message should be null");
        assertSame(cause, exception.getCause(),
            "Cause should be the same instance");
    }

    /**
     * Test constructor with both null message and cause.
     * Verifies exception can be created with both null.
     */
    @Test
    @Order(1100)
    void testMessageAndCauseConstructorWithBothNull() {
        ReportsServiceException exception = new ReportsServiceException(null, null);

        assertNotNull(exception, "Exception should not be null");
        assertNull(exception.getMessage(), "Message should be null");
        assertNull(exception.getCause(), "Cause should be null");
    }

    /**
     * Test exception can be thrown and caught.
     * Verifies the exception behaves correctly in throw/catch scenarios.
     */
    @Test
    @Order(1200)
    void testThrowAndCatch() {
        String expectedMessage = "Test throw and catch";

        ReportsServiceException thrown = assertThrows(
            ReportsServiceException.class,
            () -> {
                throw new ReportsServiceException(expectedMessage);
            },
            "Should throw ReportsServiceException"
        );

        assertEquals(expectedMessage, thrown.getMessage(),
            "Caught exception message should match");
    }

    /**
     * Test exception can be caught as RuntimeException.
     * Verifies inheritance hierarchy.
     */
    @Test
    @Order(1300)
    void testCatchAsRuntimeException() {
        String expectedMessage = "Caught as RuntimeException";

        RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> {
                throw new ReportsServiceException(expectedMessage);
            },
            "Should be catchable as RuntimeException"
        );

        assertTrue(thrown instanceof ReportsServiceException,
            "Should be instance of ReportsServiceException");
        assertEquals(expectedMessage, thrown.getMessage(),
            "Message should match when caught as RuntimeException");
    }

    /**
     * Test exception with cause chain.
     * Verifies cause chain is preserved correctly.
     */
    @Test
    @Order(1400)
    void testCauseChain() {
        IOException rootCause = new IOException("Root cause");
        SQLException intermediateCause = new SQLException("Intermediate", rootCause);
        ReportsServiceException exception = new ReportsServiceException(
            "Top level error", intermediateCause);

        assertNotNull(exception, "Exception should not be null");
        assertSame(intermediateCause, exception.getCause(),
            "Direct cause should match");
        assertSame(rootCause, exception.getCause().getCause(),
            "Root cause should match");
    }

    /**
     * Test exception in real-world scenario.
     * Simulates wrapping a database exception in a ReportsServiceException.
     */
    @Test
    @Order(1500)
    void testRealWorldScenario() {
        // Simulate a database operation failing
        SQLException dbError = new SQLException(
            "Failed to update report", "08006", 1234);

        ReportsServiceException exception = new ReportsServiceException(
            "Unable to update cross-summary report for entity 12345", dbError);

        assertNotNull(exception, "Exception should not be null");
        assertTrue(exception.getMessage().contains("cross-summary"),
            "Message should describe the operation");
        assertTrue(exception.getMessage().contains("12345"),
            "Message should include entity ID");
        assertSame(dbError, exception.getCause(),
            "Should preserve original SQLException");

        // Verify we can access SQLException details through the cause
        SQLException sqlCause = (SQLException) exception.getCause();
        assertEquals("08006", sqlCause.getSQLState(),
            "SQL state should be accessible");
        assertEquals(1234, sqlCause.getErrorCode(),
            "Error code should be accessible");
    }

    /**
     * Test exception serialization compatibility.
     * Verifies the exception extends RuntimeException properly.
     */
    @Test
    @Order(1600)
    void testSerializableCompatibility() {
        ReportsServiceException exception = new ReportsServiceException(
            "Test serializable", new IOException("IO error"));

        // RuntimeException implements Serializable, so ReportsServiceException should too
        assertTrue(exception instanceof java.io.Serializable,
            "Should be Serializable through RuntimeException");
    }

    /**
     * Test exception with complex message formatting.
     * Verifies various message formats are preserved correctly.
     */
    @Test
    @Order(1700)
    void testComplexMessageFormatting() {
        String complexMessage = String.format(
            "Failed to process report [type=%s, entityId=%d, timestamp=%d]: %s",
            "CROSS_SUMMARY", 123456L, System.currentTimeMillis(),
            "Database connection timeout");

        ReportsServiceException exception = new ReportsServiceException(complexMessage);

        assertEquals(complexMessage, exception.getMessage(),
            "Complex formatted message should be preserved exactly");
        assertTrue(exception.getMessage().contains("CROSS_SUMMARY"),
            "Should contain report type");
        assertTrue(exception.getMessage().contains("123456"),
            "Should contain entity ID");
    }
}
