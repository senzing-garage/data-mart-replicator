package com.senzing.listener.communication.sql;

import com.senzing.listener.service.MessageProcessor;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.Connector;
import com.senzing.sql.PoolConnectionProvider;
import com.senzing.util.AccessToken;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract base class for SQLConsumer tests providing shared test logic
 * for both PostgreSQL and SQLite implementations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
abstract class AbstractSQLConsumerTest {

    protected ConnectionPool connectionPool;
    protected PoolConnectionProvider connectionProvider;
    protected AccessToken providerToken;
    protected String providerName;
    protected SQLClient sqlClient;

    /**
     * Creates the database-specific connector.
     */
    protected abstract Connector createConnector() throws Exception;

    /**
     * Creates the database-specific SQLClient.
     */
    protected abstract SQLClient createSQLClient();

    /**
     * Cleans up any database-specific resources.
     */
    protected abstract void cleanupDatabase() throws Exception;

    /**
     * Gets the provider name for this test.
     */
    protected abstract String getProviderName();

    @BeforeAll
    void setUp() throws Exception {
        Connector connector = createConnector();
        this.connectionPool = new ConnectionPool(connector, 2, 5);
        this.connectionProvider = new PoolConnectionProvider(this.connectionPool);
        this.providerName = getProviderName();
        this.providerToken = ConnectionProvider.REGISTRY.bind(this.providerName, this.connectionProvider);
        this.sqlClient = createSQLClient();
    }

    @AfterAll
    void tearDown() throws Exception {
        // Unbind the provider
        if (this.providerToken != null && this.providerName != null) {
            try {
                ConnectionProvider.REGISTRY.unbind(this.providerName, this.providerToken);
            } catch (Exception ignore) {
                // ignore
            }
        }

        // Shutdown the pool
        if (this.connectionPool != null) {
            this.connectionPool.shutdown();
        }

        // Cleanup database-specific resources
        cleanupDatabase();
    }

    // ========================================================================
    // Schema Tests (Ordered)
    // ========================================================================

    /**
     * Test 1: Create schema on empty database with recreate=false.
     */
    @Test
    @Order(100)
    void testEnsureSchemaOnEmptyDatabase() throws SQLException {
        try (Connection conn = connectionProvider.getConnection()) {
            conn.setAutoCommit(false);

            // Ensure schema with recreate=false
            final Connection finalConn = conn;
            assertDoesNotThrow(() -> sqlClient.ensureSchema(finalConn, false));

            // Verify table exists
            verifySchemaExists(conn);
        }
    }

    /**
     * Test 2: Insert data, then ensureSchema with recreate=true, verify data is gone.
     */
    @Test
    @Order(200)
    void testEnsureSchemaWithRecreateTrue() throws SQLException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Insert test data
            sqlClient.insertMessage(conn, "{\"test\": \"message1\"}");
            sqlClient.insertMessage(conn, "{\"test\": \"message2\"}");
            conn.commit();

            // Verify data exists
            int count = sqlClient.getMessageCount(conn);
            assertTrue(count >= 2, "Should have at least 2 messages before recreate");

            // Ensure schema with recreate=true
            sqlClient.ensureSchema(conn, true);

            // Verify data is gone
            count = sqlClient.getMessageCount(conn);
            assertEquals(0, count, "Table should be empty after recreate");

            // Verify schema still exists
            verifySchemaExists(conn);

        } finally {
            if (conn != null) conn.close();
        }
    }

    /**
     * Test 3: Call ensureSchema with recreate=false on existing schema - no error.
     */
    @Test
    @Order(300)
    void testEnsureSchemaOnExistingSchemaNoRecreate() throws SQLException {
        try (Connection conn = connectionProvider.getConnection()) {
            conn.setAutoCommit(false);

            // Insert test data
            sqlClient.insertMessage(conn, "{\"test\": \"message\"}");
            conn.commit();

            int countBefore = sqlClient.getMessageCount(conn);
            assertTrue(countBefore > 0, "Should have data before ensureSchema");

            // Ensure schema with recreate=false - should not throw
            final Connection finalConn = conn;
            assertDoesNotThrow(() -> sqlClient.ensureSchema(finalConn, false));

            // Verify data is preserved
            int countAfter = sqlClient.getMessageCount(conn);
            assertEquals(countBefore, countAfter, "Data should be preserved");

            // Verify schema still exists
            verifySchemaExists(conn);

            // Clean up for subsequent tests
            sqlClient.ensureSchema(conn, true);
        }
    }

    // ========================================================================
    // SQLClient Message Operations Tests
    // ========================================================================

    @Test
    @Order(1000)
    void testInsertAndGetMessageCount() throws SQLException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Ensure clean schema
            sqlClient.ensureSchema(conn, true);

            // Insert messages
            sqlClient.insertMessage(conn, "{\"id\": 1}");
            sqlClient.insertMessage(conn, "{\"id\": 2}");
            sqlClient.insertMessage(conn, "{\"id\": 3}");
            conn.commit();

            // Verify count
            int count = sqlClient.getMessageCount(conn);
            assertEquals(3, count);

        } finally {
            if (conn != null) conn.close();
        }
    }

    @Test
    @Order(1100)
    void testIsQueueEmpty() throws SQLException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Ensure clean schema
            sqlClient.ensureSchema(conn, true);

            // Queue should be empty
            assertTrue(sqlClient.isQueueEmpty(conn));

            // Insert a message
            sqlClient.insertMessage(conn, "{\"test\": \"data\"}");
            conn.commit();

            // Queue should not be empty
            assertFalse(sqlClient.isQueueEmpty(conn));

        } finally {
            if (conn != null) conn.close();
        }
    }

    @Test
    @Order(1200)
    void testLeaseAndGetLeasedMessages() throws SQLException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Ensure clean schema
            sqlClient.ensureSchema(conn, true);

            // Insert messages
            sqlClient.insertMessage(conn, "{\"id\": 1}");
            sqlClient.insertMessage(conn, "{\"id\": 2}");
            sqlClient.insertMessage(conn, "{\"id\": 3}");
            conn.commit();

            // Lease messages
            String leaseId = "test-lease-" + System.currentTimeMillis();
            int leaseTime = 300; // 5 minutes
            int leaseCount = sqlClient.leaseMessages(conn, leaseId, leaseTime, 2);
            conn.commit();

            assertEquals(2, leaseCount, "Should lease exactly 2 messages");

            // Get leased messages
            List<LeasedMessage> messages = sqlClient.getLeasedMessages(conn, leaseId);
            assertEquals(2, messages.size());

            // Verify message properties
            for (LeasedMessage msg : messages) {
                assertNotNull(msg.getMessageText());
                assertEquals(leaseId, msg.getLeaseId());
                assertTrue(msg.getLeaseExpiration() > System.currentTimeMillis());
            }

        } finally {
            if (conn != null) conn.close();
        }
    }

    @Test
    @Order(1300)
    void testDeleteMessage() throws SQLException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Ensure clean schema
            sqlClient.ensureSchema(conn, true);

            // Insert and lease a message
            sqlClient.insertMessage(conn, "{\"delete\": \"test\"}");
            conn.commit();

            String leaseId = "delete-test-" + System.currentTimeMillis();
            sqlClient.leaseMessages(conn, leaseId, 300, 1);
            conn.commit();

            List<LeasedMessage> messages = sqlClient.getLeasedMessages(conn, leaseId);
            assertEquals(1, messages.size());

            LeasedMessage msg = messages.get(0);

            // Delete the message
            boolean deleted = sqlClient.deleteMessage(conn, msg.getMessageId(), leaseId);
            conn.commit();

            assertTrue(deleted, "Message should be deleted");

            // Verify queue is empty
            assertTrue(sqlClient.isQueueEmpty(conn));

        } finally {
            if (conn != null) conn.close();
        }
    }

    @Test
    @Order(1400)
    void testRenewLease() throws SQLException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Ensure clean schema
            sqlClient.ensureSchema(conn, true);

            // Insert and lease a message
            sqlClient.insertMessage(conn, "{\"renew\": \"test\"}");
            conn.commit();

            String leaseId = "renew-test-" + System.currentTimeMillis();
            sqlClient.leaseMessages(conn, leaseId, 10, 1); // 10 second lease
            conn.commit();

            List<LeasedMessage> messages = sqlClient.getLeasedMessages(conn, leaseId);
            LeasedMessage msg = messages.get(0);
            long originalExpiration = msg.getLeaseExpiration();

            // Wait a moment
            try { Thread.sleep(100); } catch (InterruptedException ignore) {}

            // Renew the lease
            long newExpiration = sqlClient.renewLease(conn, msg, 300); // 5 minute lease
            conn.commit();

            assertTrue(newExpiration > originalExpiration, "New expiration should be later");
            assertEquals(newExpiration, msg.getLeaseExpiration(), "Message should be updated");

        } finally {
            if (conn != null) conn.close();
        }
    }

    @Test
    @Order(1500)
    void testReleaseExpiredLeases() throws SQLException, InterruptedException {
        Connection conn = null;
        try {
            conn = connectionProvider.getConnection();
            conn.setAutoCommit(false);

            // Ensure clean schema
            sqlClient.ensureSchema(conn, true);

            // Insert messages
            sqlClient.insertMessage(conn, "{\"expire\": \"test\"}");
            conn.commit();

            // Lease with very short lease time (1 second)
            String leaseId = "expire-test-" + System.currentTimeMillis();
            sqlClient.leaseMessages(conn, leaseId, 1, 1);
            conn.commit();

            // Verify message is leased
            List<LeasedMessage> messages = sqlClient.getLeasedMessages(conn, leaseId);
            assertEquals(1, messages.size());

            // Wait for lease to expire
            Thread.sleep(2000);

            // Release expired leases
            int released = sqlClient.releaseExpiredLeases(conn, 1);
            conn.commit();

            assertEquals(1, released, "Should release 1 expired lease");

            // Verify message is no longer leased to original lease ID
            messages = sqlClient.getLeasedMessages(conn, leaseId);
            assertEquals(0, messages.size());

        } finally {
            if (conn != null) conn.close();
        }
    }

    // ========================================================================
    // SQLConsumer Initialization Tests
    // ========================================================================

    @Test
    @Order(2000)
    void testSQLConsumerInitWithConnectionProvider() throws Exception {
        // Ensure clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        assertDoesNotThrow(() -> consumer.init(config));

        // Verify consumer is initialized
        assertNotNull(consumer.getSQLClient());
        assertNotNull(consumer.getMessageQueue());

        // Note: We don't call destroy() here because consume() was never called,
        // and destroy() requires the processing thread to be initialized
    }

    @Test
    @Order(2100)
    void testSQLConsumerInitWithCleanDatabaseTrue() throws Exception {
        // First insert some data
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, false);
        sqlClient.insertMessage(conn, "{\"clean\": \"test\"}");
        conn.commit();
        int countBefore = sqlClient.getMessageCount(conn);
        conn.close();

        assertTrue(countBefore > 0, "Should have data before clean");

        // Initialize with cleanDatabase=true
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        // Verify data was cleaned
        conn = connectionProvider.getConnection();
        int countAfter = sqlClient.getMessageCount(conn);
        conn.close();

        assertEquals(0, countAfter, "Data should be cleaned");

        // Note: We don't call destroy() here because consume() was never called
    }

    @Test
    @Order(2200)
    void testSQLConsumerInitParameters() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.LEASE_TIME_KEY, 60);
        builder.add(SQLConsumer.MAXIMUM_LEASE_COUNT_KEY, 50);
        builder.add(SQLConsumer.MAXIMUM_SLEEP_TIME_KEY, 5);
        builder.add(SQLConsumer.MAXIMUM_RETRIES_KEY, 3);
        builder.add(SQLConsumer.RETRY_WAIT_TIME_KEY, 500);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        assertEquals(60, consumer.getLeaseTime());
        assertEquals(50, consumer.getMaximumLeaseCount());
        assertEquals(5, consumer.getMaximumSleepTime());
        assertEquals(3, consumer.getMaximumRetries());
        assertEquals(500, consumer.getRetryWaitTime());

        // Note: We don't call destroy() here because consume() was never called
    }

    @Test
    @Order(2300)
    void testSQLConsumerWithQueueRegistry() throws Exception {
        String queueName = "test-queue-" + System.currentTimeMillis();

        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.QUEUE_REGISTRY_NAME_KEY, queueName);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        // Verify queue is registered
        assertTrue(SQLConsumer.MESSAGE_QUEUE_REGISTRY.isBound(queueName));

        SQLConsumer.MessageQueue queue = SQLConsumer.MESSAGE_QUEUE_REGISTRY.lookup(queueName);
        assertNotNull(queue);
        assertSame(consumer, queue.getSQLConsumer());

        // Start consuming to enable proper destroy - use no-op processor
        CountDownLatch startedLatch = new CountDownLatch(1);
        Thread consumeThread = new Thread(() -> {
            startedLatch.countDown();
            try {
                consumer.consume((msg) -> {});
            } catch (Exception ignore) {
                // Expected during destroy
            }
        });
        consumeThread.start();

        // Wait for consumer thread to start
        startedLatch.await(5, TimeUnit.SECONDS);
        Thread.sleep(100); // Small delay to ensure consume() has initialized

        consumer.destroy();
        consumeThread.join(5000);

        // Verify queue is unregistered after destroy
        assertFalse(SQLConsumer.MESSAGE_QUEUE_REGISTRY.isBound(queueName));
    }

    // ========================================================================
    // Message Queue Interface Tests
    // ========================================================================

    @Test
    @Order(3000)
    void testMessageQueueEnqueueAndCount() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        SQLConsumer.MessageQueue queue = consumer.getMessageQueue();

        // Enqueue messages
        queue.enqueueMessage("{\"queue\": \"test1\"}");
        queue.enqueueMessage("{\"queue\": \"test2\"}");
        queue.enqueueMessage("{\"queue\": \"test3\"}");

        // Verify count
        assertEquals(3, queue.getMessageCount());
        assertFalse(queue.isEmpty());

        // Note: We don't call destroy() here because consume() was never called
    }

    // ========================================================================
    // Consume and Process Tests
    // ========================================================================

    @Test
    @Order(4000)
    void testConsumeMessages() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.LEASE_TIME_KEY, 60);
        builder.add(SQLConsumer.MAXIMUM_LEASE_COUNT_KEY, 10);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        SQLConsumer.MessageQueue queue = consumer.getMessageQueue();

        // Enqueue messages
        int messageCount = 5;
        for (int i = 0; i < messageCount; i++) {
            queue.enqueueMessage("{\"id\": " + i + "}");
        }

        // Create a processor that tracks processed messages
        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(messageCount);

        MessageProcessor processor = (message) -> {
            processedMessages.add(message);
            latch.countDown();
        };

        // Start consuming in a separate thread (consume() is a blocking call)
        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume(processor);
            } catch (Exception ignore) {
                // Expected during destroy
            }
        });
        consumeThread.start();

        // Wait for messages to be processed
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");

        // Verify all messages were processed
        assertEquals(messageCount, processedMessages.size());

        // Small delay to allow cleanup operations to complete
        Thread.sleep(500);

        // Verify queue is empty
        assertTrue(queue.isEmpty(), "Queue should be empty after processing all messages");

        consumer.destroy();
        consumeThread.join(5000);
    }

    @Test
    @Order(4100)
    void testLeaseExpirationAndReprocessing() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        // Use a very short lease time (2 seconds)
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.LEASE_TIME_KEY, 2); // 2 second lease
        builder.add(SQLConsumer.MAXIMUM_LEASE_COUNT_KEY, 1);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        SQLConsumer.MessageQueue queue = consumer.getMessageQueue();

        // Enqueue a single message
        queue.enqueueMessage("{\"slow\": \"processing\"}");

        // Track how many times the message is processed
        AtomicInteger processCount = new AtomicInteger(0);
        CountDownLatch firstProcessLatch = new CountDownLatch(1);
        CountDownLatch secondProcessLatch = new CountDownLatch(2);

        MessageProcessor slowProcessor = (message) -> {
            int count = processCount.incrementAndGet();
            firstProcessLatch.countDown();
            secondProcessLatch.countDown();

            if (count == 1) {
                // First processing: sleep longer than lease time
                // This simulates a slow processor that loses its lease
                try {
                    Thread.sleep(5000); // 5 seconds - longer than 2 second lease
                } catch (InterruptedException ignore) {}
            }
            // Second processing: complete quickly
        };

        // Start consuming in a separate thread (consume() is a blocking call)
        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume(slowProcessor);
            } catch (Exception ignore) {
                // Expected during destroy
            }
        });
        consumeThread.start();

        // Wait for at least 2 processing attempts (message should be reprocessed after lease expires)
        secondProcessLatch.await(15, TimeUnit.SECONDS);

        // The message should have been processed at least twice due to lease expiration
        assertTrue(processCount.get() >= 2,
                "Message should be reprocessed after lease expires. Actual count: " + processCount.get());

        consumer.destroy();
        consumeThread.join(5000);
    }

    // ========================================================================
    // Exception Handling and Failure Injection Tests
    // ========================================================================

    @Test
    @Order(5000)
    void testInitWithInvalidConnectionProvider() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, "non-existent-provider-" + System.currentTimeMillis());
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();

        // Should throw MessageConsumerSetupException due to NameNotFoundException
        Exception exception = assertThrows(Exception.class, () -> consumer.init(config));
        assertTrue(exception.getMessage().contains("ConnectionProvider") ||
                   exception.getCause() != null,
                   "Should fail due to invalid connection provider");
    }

    @Test
    @Order(5100)
    void testHandleFailureWithRetries() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        // Configure with 3 retries and short retry wait time
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.MAXIMUM_RETRIES_KEY, 3);
        builder.add(SQLConsumer.RETRY_WAIT_TIME_KEY, 100); // 100ms retry wait
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        // Suppress console output from failure logging
        new SystemErr().execute(() -> {
            // Test handleFailure with failure count below max retries
            long startTime = System.currentTimeMillis();
            boolean shouldAbort = consumer.handleFailure(1, new SQLException("Test failure"));
            long elapsed = System.currentTimeMillis() - startTime;

            assertFalse(shouldAbort, "Should not abort when failure count is below max retries");
            assertTrue(elapsed >= 90, "Should have waited approximately retry wait time");
        });
    }

    @Test
    @Order(5200)
    void testHandleFailureExceedsMaxRetries() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        // Configure with 2 retries
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.MAXIMUM_RETRIES_KEY, 2);
        builder.add(SQLConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        // Suppress console output from failure logging
        new SystemErr().execute(() -> {
            // Test handleFailure with failure count exceeding max retries
            boolean shouldAbort = consumer.handleFailure(3, new SQLException("Test failure"));

            assertTrue(shouldAbort, "Should abort when failure count exceeds max retries");
        });
    }

    @Test
    @Order(5300)
    void testGenerateLeaseId() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        // Generate multiple lease IDs and verify they are unique and well-formed
        String leaseId1 = consumer.generateLeaseId();
        String leaseId2 = consumer.generateLeaseId();
        String leaseId3 = consumer.generateLeaseId();

        // Verify not null and not empty
        assertNotNull(leaseId1);
        assertNotNull(leaseId2);
        assertNotNull(leaseId3);
        assertFalse(leaseId1.isEmpty());

        // Verify uniqueness
        assertNotEquals(leaseId1, leaseId2);
        assertNotEquals(leaseId2, leaseId3);
        assertNotEquals(leaseId1, leaseId3);

        // Verify format contains expected parts (pid|timestamp|random)
        assertTrue(leaseId1.contains("|"), "Lease ID should contain pipe separators");
        String[] parts = leaseId1.split("\\|");
        assertEquals(3, parts.length, "Lease ID should have 3 parts");
    }

    @Test
    @Order(5400)
    void testConsumeWithDatabaseFailureTriggeringAbort() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        // Configure with 0 retries so first failure triggers abort
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.MAXIMUM_RETRIES_KEY, 0); // No retries - abort on first failure
        builder.add(SQLConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        FailureInjectingSQLConsumer consumer = new FailureInjectingSQLConsumer();
        consumer.init(config);

        // Enqueue a message
        SQLConsumer.MessageQueue queue = consumer.getMessageQueue();
        queue.enqueueMessage("{\"test\": \"failure\"}");

        // Set up failure injection - fail on first getConnection call during consumption
        consumer.setFailOnGetConnection(true);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch destroyedLatch = new CountDownLatch(1);

        // Suppress console output from failure logging
        new SystemErr().execute(() -> {
            // Start consuming - should abort due to failure
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume((msg) -> processedCount.incrementAndGet());
                } catch (Exception ignore) {
                    // Expected
                } finally {
                    destroyedLatch.countDown();
                }
            });
            consumeThread.start();

            // Wait for consumption to abort (should be quick due to 0 retries)
            boolean finished = destroyedLatch.await(10, TimeUnit.SECONDS);
            assertTrue(finished, "Consumer should abort within timeout");

            // Message should not have been processed due to failure before processing
            assertEquals(0, processedCount.get(), "No messages should be processed when getConnection fails");

            // Always destroy and join to ensure thread completes within execute() block
            consumer.destroy();
            consumeThread.join(5000);
        });
    }

    @Test
    @Order(5500)
    void testConsumeWithTransientFailureAndRetry() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        // Configure with 3 retries
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, false);
        builder.add(SQLConsumer.MAXIMUM_RETRIES_KEY, 3);
        builder.add(SQLConsumer.RETRY_WAIT_TIME_KEY, 50); // 50ms retry
        builder.add(SQLConsumer.LEASE_TIME_KEY, 60);
        JsonObject config = builder.build();

        FailureInjectingSQLConsumer consumer = new FailureInjectingSQLConsumer();
        consumer.init(config);

        // Enqueue messages
        SQLConsumer.MessageQueue queue = consumer.getMessageQueue();
        queue.enqueueMessage("{\"test\": \"recovery\"}");

        // Set up transient failure - fail twice, then succeed
        consumer.setFailureCount(2);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch processedLatch = new CountDownLatch(1);

        // Suppress console output from failure logging
        new SystemErr().execute(() -> {
            // Start consuming
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume((msg) -> {
                        processedCount.incrementAndGet();
                        processedLatch.countDown();
                    });
                } catch (Exception ignore) {
                    // Expected during destroy
                }
            });
            consumeThread.start();

            // Wait for message to be processed after retries
            boolean processed = processedLatch.await(15, TimeUnit.SECONDS);
            assertTrue(processed, "Message should eventually be processed after transient failures");
            assertEquals(1, processedCount.get());

            // Verify handleFailure was called (failure count should have been tracked)
            assertTrue(consumer.getHandleFailureCallCount() >= 2,
                    "handleFailure should have been called at least twice");

            consumer.destroy();
            consumeThread.join(5000);
        });
    }

    @Test
    @Order(5600)
    void testDefaultConfigurationValues() throws Exception {
        // Clean schema first
        Connection conn = connectionProvider.getConnection();
        conn.setAutoCommit(false);
        sqlClient.ensureSchema(conn, true);
        conn.close();

        // Initialize with minimal config to test defaults
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.providerName);
        JsonObject config = builder.build();

        SQLConsumer consumer = new SQLConsumer();
        consumer.init(config);

        // Verify default values are applied
        assertEquals(SQLConsumer.DEFAULT_LEASE_TIME, consumer.getLeaseTime());
        assertEquals(SQLConsumer.DEFAULT_MAXIMUM_LEASE_COUNT, consumer.getMaximumLeaseCount());
        assertEquals(SQLConsumer.DEFAULT_MAXIMUM_SLEEP_TIME, consumer.getMaximumSleepTime());
        assertEquals(SQLConsumer.DEFAULT_MAXIMUM_RETRIES, consumer.getMaximumRetries());
        assertEquals(SQLConsumer.DEFAULT_RETRY_WAIT_TIME, consumer.getRetryWaitTime());
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /**
     * Verifies the schema exists by checking for the sz_message_queue table.
     */
    protected void verifySchemaExists(Connection conn) throws SQLException {
        // Try to query the table
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sz_message_queue")) {
            assertTrue(rs.next(), "Should be able to query sz_message_queue table");
        }
    }

    /**
     * Simple MessageProcessor for testing.
     */
    protected static class TestMessageProcessor implements MessageProcessor {
        private final List<JsonObject> messages = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch latch;
        private final long processingDelay;

        public TestMessageProcessor(int expectedCount) {
            this(expectedCount, 0);
        }

        public TestMessageProcessor(int expectedCount, long processingDelayMs) {
            this.latch = new CountDownLatch(expectedCount);
            this.processingDelay = processingDelayMs;
        }

        @Override
        public void process(JsonObject message) throws ServiceExecutionException {
            if (processingDelay > 0) {
                try {
                    Thread.sleep(processingDelay);
                } catch (InterruptedException ignore) {}
            }
            messages.add(message);
            latch.countDown();
        }

        public List<JsonObject> getMessages() {
            return messages;
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
    }

    // ========================================================================
    // Helper Classes for Exception Injection Testing
    // ========================================================================

    /**
     * A SQLConsumer subclass that can inject failures for testing error handling paths.
     */
    protected static class FailureInjectingSQLConsumer extends SQLConsumer {
        private volatile boolean failOnGetConnection = false;
        private volatile int failuresRemaining = 0;
        private final AtomicInteger handleFailureCallCount = new AtomicInteger(0);

        public void setFailOnGetConnection(boolean fail) {
            this.failOnGetConnection = fail;
        }

        public void setFailureCount(int count) {
            this.failuresRemaining = count;
        }

        public int getHandleFailureCallCount() {
            return handleFailureCallCount.get();
        }

        @Override
        protected Connection getConnection() throws SQLException {
            if (failOnGetConnection) {
                throw new SQLException("Injected failure for testing");
            }
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new SQLException("Transient failure for testing (" + failuresRemaining + " remaining)");
            }
            return super.getConnection();
        }

        @Override
        protected boolean handleFailure(int failureCount, Exception failure) {
            handleFailureCallCount.incrementAndGet();
            return super.handleFailure(failureCount, failure);
        }
    }
}
