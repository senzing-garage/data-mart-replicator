package com.senzing.listener.communication.rabbitmq;

import com.rabbitmq.client.*;
import com.senzing.listener.communication.sql.LeasedMessage;
import com.senzing.listener.communication.sql.SQLiteClient;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.Connector;
import com.senzing.sql.SQLiteConnector;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mock implementation of {@link Channel} that uses SQLite as the backing
 * message queue storage. This allows testing {@link RabbitMQConsumer} without
 * requiring an actual RabbitMQ connection.
 */
public class MockRabbitMQChannel implements Channel {

    private final ConnectionPool connectionPool;
    private final SQLiteClient sqlClient;
    private final File tempDbFile;
    private final int visibilityTimeoutSeconds;
    private final AtomicLong deliveryTagCounter = new AtomicLong(0);
    private final ConcurrentHashMap<Long, Long> deliveryTagToMessageId = new ConcurrentHashMap<>();

    private volatile boolean closed = false;
    private volatile DeliverCallback currentDeliverCallback;
    private volatile String currentConsumerTag;
    private volatile Thread consumerThread;
    private final AtomicBoolean consuming = new AtomicBoolean(false);
    private final AtomicInteger basicCancelCallCount = new AtomicInteger(0);

    // Exception injection fields
    private volatile boolean throwOnBasicConsume = false;
    private volatile boolean throwOnBasicCancel = false;
    private volatile boolean throwOnBasicAck = false;
    private volatile int throwOnQueueDeclareCount = 0;
    private final AtomicInteger queueDeclareCallCount = new AtomicInteger(0);

    /**
     * Creates a new MockRabbitMQChannel with default visibility timeout of 30 seconds.
     */
    public MockRabbitMQChannel() throws Exception {
        this(30);
    }

    /**
     * Creates a new MockRabbitMQChannel with the specified visibility timeout.
     *
     * @param visibilityTimeoutSeconds The visibility timeout in seconds.
     */
    public MockRabbitMQChannel(int visibilityTimeoutSeconds) throws Exception {
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;

        // Create a temporary SQLite database
        this.tempDbFile = File.createTempFile("mock_rabbitmq_", ".db");
        this.tempDbFile.deleteOnExit();

        Connector connector = new SQLiteConnector(tempDbFile.getAbsolutePath());
        this.connectionPool = new ConnectionPool(connector, 2, 5);
        this.sqlClient = new SQLiteClient();

        // Initialize the schema
        try (Connection conn = connectionPool.acquire()) {
            conn.setAutoCommit(false);
            sqlClient.ensureSchema(conn, true);
            conn.commit();
        }
    }

    /**
     * Enqueues a message to the mock queue.
     *
     * @param messageBody The message body to enqueue.
     */
    public void enqueueMessage(String messageBody) throws SQLException {
        try (Connection conn = connectionPool.acquire()) {
            conn.setAutoCommit(false);
            sqlClient.insertMessage(conn, messageBody);
            conn.commit();
        }
    }

    /**
     * Gets the count of messages in the queue.
     */
    public long getMessageCount() throws SQLException {
        try (Connection conn = connectionPool.acquire()) {
            return sqlClient.getMessageCount(conn);
        }
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
            CancelCallback cancelCallback) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, null, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
            boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
            CancelCallback cancelCallback) throws IOException {

        if (closed) {
            throw new IOException("Channel is closed");
        }

        if (throwOnBasicConsume) {
            throw new IOException("Injected basicConsume failure");
        }

        this.currentDeliverCallback = deliverCallback;
        this.currentConsumerTag = "mock-consumer-" + System.currentTimeMillis();
        this.consuming.set(true);

        // Start a background thread to poll and deliver messages
        consumerThread = new Thread(() -> {
            while (consuming.get() && !closed) {
                try {
                    // Poll for messages
                    try (Connection conn = connectionPool.acquire()) {
                        conn.setAutoCommit(false);

                        // Release expired leases
                        sqlClient.releaseExpiredLeases(conn, visibilityTimeoutSeconds);
                        conn.commit();

                        // Generate a unique lease ID
                        String leaseId = "mock-lease-" + System.currentTimeMillis() + "-" +
                                deliveryTagCounter.incrementAndGet();

                        // Lease messages
                        sqlClient.leaseMessages(conn, leaseId, visibilityTimeoutSeconds, 1);
                        conn.commit();

                        // Get leased messages
                        List<LeasedMessage> leasedMessages = sqlClient.getLeasedMessages(conn, leaseId);

                        for (LeasedMessage lm : leasedMessages) {
                            if (!consuming.get() || closed) break;

                            long deliveryTag = deliveryTagCounter.incrementAndGet();
                            deliveryTagToMessageId.put(deliveryTag, lm.getMessageId());

                            // Create delivery
                            Envelope envelope = new Envelope(deliveryTag, false, "", queue);
                            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().build();
                            Delivery delivery = new Delivery(envelope, props,
                                    lm.getMessageText().getBytes(StandardCharsets.UTF_8));

                            // Deliver the message
                            if (currentDeliverCallback != null && consuming.get()) {
                                try {
                                    currentDeliverCallback.handle(currentConsumerTag, delivery);
                                } catch (Exception e) {
                                    // Log but continue
                                    System.err.println("Error in deliver callback: " + e.getMessage());
                                }
                            }
                        }
                    }

                    // Small delay between polls
                    Thread.sleep(100);

                } catch (SQLException e) {
                    // Log but continue
                    if (consuming.get()) {
                        System.err.println("Database error in consumer thread: " + e.getMessage());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        consumerThread.start();

        return this.currentConsumerTag;
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        if (closed) {
            throw new IOException("Channel is closed");
        }

        if (throwOnBasicAck) {
            throw new IOException("Injected basicAck failure");
        }

        Long messageId = deliveryTagToMessageId.remove(deliveryTag);
        if (messageId != null) {
            try (Connection conn = connectionPool.acquire()) {
                conn.setAutoCommit(false);
                sqlClient.deleteMessage(conn, messageId, null);
                conn.commit();
            } catch (SQLException e) {
                throw new IOException("Failed to ack message: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void basicCancel(String consumerTag) throws IOException {
        basicCancelCallCount.incrementAndGet();
        if (throwOnBasicCancel) {
            throw new IOException("Injected basicCancel failure");
        }
        consuming.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            consumerThread = null;
        }
        currentDeliverCallback = null;
        currentConsumerTag = null;
    }

    /**
     * Gets the number of times basicCancel was called.
     *
     * @return The call count.
     */
    public int getBasicCancelCallCount() {
        return this.basicCancelCallCount.get();
    }

    // ========================================================================
    // Exception Injection Methods
    // ========================================================================

    /**
     * Sets whether basicConsume should throw an IOException.
     *
     * @param throwException Whether to throw an exception.
     */
    public void setThrowOnBasicConsume(boolean throwException) {
        this.throwOnBasicConsume = throwException;
    }

    /**
     * Sets whether basicCancel should throw an IOException.
     *
     * @param throwException Whether to throw an exception.
     */
    public void setThrowOnBasicCancel(boolean throwException) {
        this.throwOnBasicCancel = throwException;
    }

    /**
     * Sets whether basicAck should throw an IOException.
     *
     * @param throwException Whether to throw an exception.
     */
    public void setThrowOnBasicAck(boolean throwException) {
        this.throwOnBasicAck = throwException;
    }

    /**
     * Sets the number of queueDeclare calls that should throw IOException.
     * For example, if set to 1, the first call throws, subsequent calls succeed.
     *
     * @param count The number of calls that should throw.
     */
    public void setThrowOnQueueDeclareCount(int count) {
        this.throwOnQueueDeclareCount = count;
        this.queueDeclareCallCount.set(0);
    }

    /**
     * Gets the number of times queueDeclare was called.
     *
     * @return The call count.
     */
    public int getQueueDeclareCallCount() {
        return this.queueDeclareCallCount.get();
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive,
            boolean autoDelete, Map<String, Object> arguments) throws IOException {
        int callNum = queueDeclareCallCount.incrementAndGet();
        if (callNum <= throwOnQueueDeclareCount) {
            throw new IOException("Injected queueDeclare failure (call " + callNum + ")");
        }
        // No-op for mock - queue is implicit via SQLite
        return new AMQP.Queue.DeclareOk.Builder()
                .queue(queue)
                .messageCount(0)
                .consumerCount(0)
                .build();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        consuming.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (connectionPool != null) {
            connectionPool.shutdown();
        }
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    // ========================================================================
    // Required interface methods with minimal or no-op implementations
    // ========================================================================

    @Override
    public int getChannelNumber() {
        return 1;
    }

    @Override
    public com.rabbitmq.client.Connection getConnection() {
        return null;
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException {
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
    public void addReturnListener(ReturnListener listener) {}

    @Override
    public ReturnListener addReturnListener(ReturnCallback returnCallback) {
        return null;
    }

    @Override
    public boolean removeReturnListener(ReturnListener listener) {
        return false;
    }

    @Override
    public void clearReturnListeners() {}

    @Override
    public void addConfirmListener(ConfirmListener listener) {}

    @Override
    public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
        return null;
    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
        return false;
    }

    @Override
    public void clearConfirmListeners() {}

    @Override
    public Consumer getDefaultConsumer() {
        return null;
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {}

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) {}

    @Override
    public void basicQos(int prefetchCount, boolean global) {}

    @Override
    public void basicQos(int prefetchCount) {}

    @Override
    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body)
            throws IOException {
        // Allow publishing to the mock queue
        try {
            enqueueMessage(new String(body, StandardCharsets.UTF_8));
        } catch (SQLException e) {
            throw new IOException("Failed to publish message", e);
        }
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory,
            AMQP.BasicProperties props, byte[] body) throws IOException {
        basicPublish(exchange, routingKey, props, body);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate,
            AMQP.BasicProperties props, byte[] body) throws IOException {
        basicPublish(exchange, routingKey, props, body);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
        return new AMQP.Exchange.DeclareOk.Builder().build();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
        return exchangeDeclare(exchange, type.getType());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
        return exchangeDeclare(exchange, type);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable)
            throws IOException {
        return exchangeDeclare(exchange, type);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable,
            boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable,
            boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable,
            boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable,
            boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type);
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete,
            boolean internal, Map<String, Object> arguments) {}

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable,
            boolean autoDelete, boolean internal, Map<String, Object> arguments) {}

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
        return new AMQP.Exchange.DeclareOk.Builder().build();
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
        return new AMQP.Exchange.DeleteOk.Builder().build();
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) {}

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
        return exchangeDelete(exchange, false);
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey)
            throws IOException {
        return new AMQP.Exchange.BindOk.Builder().build();
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey,
            Map<String, Object> arguments) throws IOException {
        return exchangeBind(destination, source, routingKey);
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey,
            Map<String, Object> arguments) {}

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey)
            throws IOException {
        return new AMQP.Exchange.UnbindOk.Builder().build();
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey,
            Map<String, Object> arguments) throws IOException {
        return exchangeUnbind(destination, source, routingKey);
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey,
            Map<String, Object> arguments) {}

    @Override
    public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
        return queueDeclare("", false, false, true, null);
    }

    @Override
    public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete,
            Map<String, Object> arguments) {}

    @Override
    public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
        return queueDeclare(queue, false, false, false, null);
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
        return new AMQP.Queue.DeleteOk.Builder().messageCount(0).build();
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
        return queueDelete(queue);
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) {}

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) throws IOException {
        return new AMQP.Queue.BindOk.Builder().build();
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey,
            Map<String, Object> arguments) throws IOException {
        return queueBind(queue, exchange, routingKey);
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey,
            Map<String, Object> arguments) {}

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) throws IOException {
        return new AMQP.Queue.UnbindOk.Builder().build();
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey,
            Map<String, Object> arguments) throws IOException {
        return queueUnbind(queue, exchange, routingKey);
    }

    @Override
    public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
        return new AMQP.Queue.PurgeOk.Builder().messageCount(0).build();
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
        return null;
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {}

    @Override
    public void basicReject(long deliveryTag, boolean requeue) {}

    @Override
    public String basicConsume(String queue, Consumer callback) throws IOException {
        return basicConsume(queue, false, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, "", callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback)
            throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback)
            throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
            boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
        // Not supported for legacy Consumer interface in mock
        throw new java.lang.UnsupportedOperationException("Use DeliverCallback version");
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback)
            throws IOException {
        return basicConsume(queue, false, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, false, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, false, deliverCallback, ct -> {});
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
            CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, deliverCallback, ct -> {});
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
            DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
            DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, arguments, deliverCallback, ct -> {});
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
            DeliverCallback deliverCallback, CancelCallback cancelCallback,
            ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
            CancelCallback cancelCallback) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
            CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, deliverCallback, ct -> {}, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
            boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
            CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
            boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
            ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, ct -> {});
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover() throws IOException {
        return new AMQP.Basic.RecoverOk.Builder().build();
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) throws IOException {
        return basicRecover();
    }

    @Override
    public AMQP.Tx.SelectOk txSelect() throws IOException {
        return new AMQP.Tx.SelectOk.Builder().build();
    }

    @Override
    public AMQP.Tx.CommitOk txCommit() throws IOException {
        return new AMQP.Tx.CommitOk.Builder().build();
    }

    @Override
    public AMQP.Tx.RollbackOk txRollback() throws IOException {
        return new AMQP.Tx.RollbackOk.Builder().build();
    }

    @Override
    public AMQP.Confirm.SelectOk confirmSelect() throws IOException {
        return new AMQP.Confirm.SelectOk.Builder().build();
    }

    @Override
    public long getNextPublishSeqNo() {
        return 0;
    }

    @Override
    public boolean waitForConfirms() {
        return true;
    }

    @Override
    public boolean waitForConfirms(long timeout) {
        return true;
    }

    @Override
    public void waitForConfirmsOrDie() {}

    @Override
    public void waitForConfirmsOrDie(long timeout) {}

    @Override
    public void asyncRpc(Method method) {}

    @Override
    public Command rpc(Method method) {
        return null;
    }

    @Override
    public long messageCount(String queue) {
        try {
            return getMessageCount();
        } catch (SQLException e) {
            return 0;
        }
    }

    @Override
    public long consumerCount(String queue) {
        return consuming.get() ? 1 : 0;
    }

    @Override
    public CompletableFuture<Command> asyncCompletableRpc(Method method) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {}

    @Override
    public void removeShutdownListener(ShutdownListener listener) {}

    @Override
    public ShutdownSignalException getCloseReason() {
        return null;
    }

    @Override
    public void notifyListeners() {}
}
