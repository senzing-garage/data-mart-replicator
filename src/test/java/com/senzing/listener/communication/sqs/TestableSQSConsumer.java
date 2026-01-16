package com.senzing.listener.communication.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * A testable subclass of {@link SQSConsumer} that allows injecting a mock
 * {@link SqsClient} for testing purposes.
 */
public class TestableSQSConsumer extends SQSConsumer {

    private SqsClient injectedClient;

    /**
     * Default constructor.
     */
    public TestableSQSConsumer() {
        super();
    }

    /**
     * Sets the {@link SqsClient} to be used instead of creating a real one.
     *
     * @param client The {@link SqsClient} to use.
     */
    public void setInjectedClient(SqsClient client) {
        this.injectedClient = client;
    }

    /**
     * Gets the injected {@link SqsClient}.
     *
     * @return The injected {@link SqsClient}, or null if not set.
     */
    public SqsClient getInjectedClient() {
        return this.injectedClient;
    }

    /**
     * Override to return the injected mock client instead of creating a real one.
     *
     * @return The injected {@link SqsClient}.
     */
    @Override
    protected SqsClient createSqsClient() {
        return this.injectedClient;
    }
}
