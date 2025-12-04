package com.senzing.listener.service.exception;

/**
 * Exception that can be thrown if a {@link 
 * com.senzing.listener.service.ListenerService}
 * operation fails.
 */
public class ServiceExecutionException extends Exception {
  /**
   * Constructs with the specified message describing why the failure occurred.
   *
   * @param message The message describing why the failure occurred.
   */
  public ServiceExecutionException(String message) {
    super(message);
  }

  /**
   * Constructs with the specified {@link Exception} describing the underlying
   * cause of the failure.
   *
   * @param e The {@link Exception} describing the underlying cause of the
   *          failure.
   */
  public ServiceExecutionException(Exception e) {
    super(e);
  }

  /**
   * Constructs with the specified message describing the failure and the
   * {@link Exception} describing the underlying cause of the failure.
   *
   * @param message The message describing why the failure occurred.
   *
   * @param e The {@link Exception} describing the underlying cause of the
   *          failure.
   */
  public ServiceExecutionException(String message, Exception e) {
    super(message, e);
  }

}
