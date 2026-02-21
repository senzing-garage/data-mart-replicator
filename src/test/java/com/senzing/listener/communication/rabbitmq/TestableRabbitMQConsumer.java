package com.senzing.listener.communication.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

/**
 * A testable subclass of {@link RabbitMQConsumer} that allows injecting a mock
 * {@link ConnectionFactory} for testing purposes.
 */
public class TestableRabbitMQConsumer extends RabbitMQConsumer {

    private ConnectionFactory injectedConnectionFactory;
    private Integer maximumPendingCountOverride;

    /**
     * Default constructor.
     */
    public TestableRabbitMQConsumer() {
        super();
    }

    /**
     * Sets the {@link ConnectionFactory} to be used instead of creating a real one.
     *
     * @param connectionFactory The {@link ConnectionFactory} to use.
     */
    public void setInjectedConnectionFactory(ConnectionFactory connectionFactory) {
        this.injectedConnectionFactory = connectionFactory;
    }

    /**
     * Gets the injected {@link ConnectionFactory}.
     *
     * @return The injected {@link ConnectionFactory}, or null if not set.
     */
    public ConnectionFactory getInjectedConnectionFactory() {
        return this.injectedConnectionFactory;
    }

    /**
     * Override to return the injected mock connection factory instead of creating a real one.
     *
     * @return The injected {@link ConnectionFactory}.
     */
    @Override
    protected ConnectionFactory createConnectionFactory() {
        return this.injectedConnectionFactory;
    }

    /**
     * Sets the maximum pending count override for testing throttling.
     *
     * @param count The maximum pending count to use.
     */
    public void setMaximumPendingCount(int count) {
        this.maximumPendingCountOverride = count;
    }

    /**
     * Override to return the configured maximum pending count if set.
     *
     * @return The maximum pending count.
     */
    @Override
    protected int getMaximumPendingCount() {
        if (this.maximumPendingCountOverride != null) {
            return this.maximumPendingCountOverride;
        }
        return super.getMaximumPendingCount();
    }
}
