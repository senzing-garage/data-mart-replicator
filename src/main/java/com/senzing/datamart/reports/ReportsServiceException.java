package com.senzing.datamart.reports;

/**
 * Provides a {@link RuntimeException} that is thrown from the
 * reports services in the event of failure.
 */
public class ReportsServiceException extends RuntimeException {
    /**
     * Default constructor.
     */
    public ReportsServiceException() {
        super();
    }

    /**
     * Constructs with the specified message.
     * 
     * @param message THe message with which to construct.
     */
    public ReportsServiceException(String message) {
        super(message);
    }

    /**
     * Constructs with the specified underlying cause.
     * 
     * @param cause The underlying cause of the failure.
     */
    public ReportsServiceException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs with the specified message and underlying cause.
     * 
     * @param message THe message with which to construct.
     * @param cause The underlying cause of the failure.
     */
    public ReportsServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
