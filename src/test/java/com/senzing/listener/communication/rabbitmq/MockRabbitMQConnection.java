package com.senzing.listener.communication.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * A mock implementation of {@link Connection} that returns a {@link MockRabbitMQChannel}.
 * This allows testing {@link RabbitMQConsumer} without requiring an actual RabbitMQ connection.
 */
public class MockRabbitMQConnection implements Connection {

    private final MockRabbitMQChannel channel;
    private volatile boolean closed = false;

    /**
     * Creates a new MockRabbitMQConnection with a new MockRabbitMQChannel.
     */
    public MockRabbitMQConnection() throws Exception {
        this(new MockRabbitMQChannel());
    }

    /**
     * Creates a new MockRabbitMQConnection with the specified channel.
     *
     * @param channel The {@link MockRabbitMQChannel} to use.
     */
    public MockRabbitMQConnection(MockRabbitMQChannel channel) {
        this.channel = channel;
    }

    /**
     * Gets the mock channel for this connection.
     *
     * @return The {@link MockRabbitMQChannel}.
     */
    public MockRabbitMQChannel getMockChannel() {
        return this.channel;
    }

    @Override
    public Channel createChannel() throws IOException {
        if (closed) {
            throw new IOException("Connection is closed");
        }
        return channel;
    }

    @Override
    public Channel createChannel(int channelNumber) throws IOException {
        return createChannel();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (channel != null) {
            channel.close();
        }
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException {
        close();
    }

    @Override
    public void close(int timeout) throws IOException {
        close();
    }

    @Override
    public void close(int closeCode, String closeMessage, int timeout) throws IOException {
        close();
    }

    @Override
    public void abort() {
        try {
            close();
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
        abort();
    }

    @Override
    public void abort(int timeout) {
        abort();
    }

    @Override
    public void abort(int closeCode, String closeMessage, int timeout) {
        abort();
    }

    @Override
    public InetAddress getAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public int getPort() {
        return 5672;
    }

    @Override
    public int getChannelMax() {
        return 0;
    }

    @Override
    public int getFrameMax() {
        return 0;
    }

    @Override
    public int getHeartbeat() {
        return 0;
    }

    @Override
    public Map<String, Object> getClientProperties() {
        return null;
    }

    @Override
    public String getClientProvidedName() {
        return "mock-connection";
    }

    @Override
    public Map<String, Object> getServerProperties() {
        return null;
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        return null;
    }

    @Override
    public String getId() {
        return "mock-connection-id";
    }

    @Override
    public void setId(String id) {
        // no-op
    }

    @Override
    public void addBlockedListener(BlockedListener listener) {
        // no-op
    }

    @Override
    public BlockedListener addBlockedListener(BlockedCallback blockedCallback,
            UnblockedCallback unblockedCallback) {
        return null;
    }

    @Override
    public boolean removeBlockedListener(BlockedListener listener) {
        return false;
    }

    @Override
    public void clearBlockedListeners() {
        // no-op
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        // no-op
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        // no-op
    }

    @Override
    public ShutdownSignalException getCloseReason() {
        return null;
    }

    @Override
    public void notifyListeners() {
        // no-op
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }
}
