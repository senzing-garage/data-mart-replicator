package com.senzing.listener.communication.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A mock implementation of {@link ConnectionFactory} that tracks configuration calls
 * and returns a {@link MockRabbitMQConnection}. This allows testing the full
 * {@link RabbitMQConsumer#createConnection()} method without requiring an actual
 * RabbitMQ connection.
 */
public class MockConnectionFactory extends ConnectionFactory {

    private final MockRabbitMQChannel mockChannel;
    private final MockRabbitMQConnection mockConnection;

    private String configuredHost;
    private Integer configuredPort;
    private String configuredVirtualHost;
    private String configuredUsername;
    private String configuredPassword;
    private int newConnectionCallCount = 0;

    /**
     * Creates a new MockConnectionFactory with a new MockRabbitMQChannel.
     */
    public MockConnectionFactory() throws Exception {
        this(new MockRabbitMQChannel());
    }

    /**
     * Creates a new MockConnectionFactory with the specified channel.
     *
     * @param channel The {@link MockRabbitMQChannel} to use.
     */
    public MockConnectionFactory(MockRabbitMQChannel channel) {
        this.mockChannel = channel;
        this.mockConnection = new MockRabbitMQConnection(channel);
    }

    /**
     * Gets the mock channel for this factory.
     *
     * @return The {@link MockRabbitMQChannel}.
     */
    public MockRabbitMQChannel getMockChannel() {
        return this.mockChannel;
    }

    /**
     * Gets the mock connection for this factory.
     *
     * @return The {@link MockRabbitMQConnection}.
     */
    public MockRabbitMQConnection getMockConnection() {
        return this.mockConnection;
    }

    @Override
    public void setHost(String host) {
        this.configuredHost = host;
        super.setHost(host);
    }

    @Override
    public void setPort(int port) {
        this.configuredPort = port;
        super.setPort(port);
    }

    @Override
    public void setVirtualHost(String virtualHost) {
        this.configuredVirtualHost = virtualHost;
        super.setVirtualHost(virtualHost);
    }

    @Override
    public void setUsername(String username) {
        this.configuredUsername = username;
        super.setUsername(username);
    }

    @Override
    public void setPassword(String password) {
        this.configuredPassword = password;
        super.setPassword(password);
    }

    @Override
    public Connection newConnection() throws IOException, TimeoutException {
        newConnectionCallCount++;
        return mockConnection;
    }

    /**
     * Gets the host that was configured via setHost().
     *
     * @return The configured host, or null if not set.
     */
    public String getConfiguredHost() {
        return this.configuredHost;
    }

    /**
     * Gets the port that was configured via setPort().
     *
     * @return The configured port, or null if not set.
     */
    public Integer getConfiguredPort() {
        return this.configuredPort;
    }

    /**
     * Gets the virtual host that was configured via setVirtualHost().
     *
     * @return The configured virtual host, or null if not set.
     */
    public String getConfiguredVirtualHost() {
        return this.configuredVirtualHost;
    }

    /**
     * Gets the username that was configured via setUsername().
     *
     * @return The configured username, or null if not set.
     */
    public String getConfiguredUsername() {
        return this.configuredUsername;
    }

    /**
     * Gets the password that was configured via setPassword().
     *
     * @return The configured password, or null if not set.
     */
    public String getConfiguredPassword() {
        return this.configuredPassword;
    }

    /**
     * Gets the number of times newConnection() was called.
     *
     * @return The call count.
     */
    public int getNewConnectionCallCount() {
        return this.newConnectionCallCount;
    }
}
