package com.senzing.listener.communication.exception;

/**
 * Exception thrown when message consumer fails initialization.
 */
public class MessageConsumerSetupException extends MessageConsumerException {
  /**
   * Constructs with the specified message.
   * 
   * @param message The message with which to construct.
   */
  public MessageConsumerSetupException(String message) {
    super(message);
  }

  /**
   * Constructs with the specified {@link Exception} describing the
   * underlying failure that occurred.
   *
   * @param cause The {@link Exception} describing the underlying failure
   *              that occurred.
   */
  public MessageConsumerSetupException(Exception cause) {
    super(cause);
  }

  /**
   * Constructs with the specified message and {@link Exception}
   * describing the underlying failure that occurred.
   *
   * @param message The message with which to construct.
   * @param cause   The {@link Exception} describing the underlying failure
   *                that occurred.
   */
  public MessageConsumerSetupException(String message,
      Exception cause) {
    super(message, cause);
  }
}
