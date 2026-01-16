package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.communication.sql.SQLConsumer;
import com.senzing.listener.communication.sqs.SQSConsumer;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.Connector;
import com.senzing.sql.PoolConnectionProvider;
import com.senzing.sql.SQLiteConnector;
import com.senzing.util.AccessToken;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import com.senzing.sql.PostgreSqlConnector;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MessageConsumerFactory}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class MessageConsumerFactoryTest {

    private EmbeddedPostgres embeddedPostgres;
    private ConnectionPool postgresPool;
    private PoolConnectionProvider postgresProvider;
    private AccessToken postgresProviderToken;
    private String postgresProviderName;

    private File sqliteDbFile;
    private ConnectionPool sqlitePool;
    private PoolConnectionProvider sqliteProvider;
    private AccessToken sqliteProviderToken;
    private String sqliteProviderName;

    /**
     * Safely destroys a consumer, ignoring any NPE from unstarted consumers.
     */
    private void safeDestroy(MessageConsumer consumer) {
        if (consumer != null) {
            try {
                consumer.destroy();
            } catch (NullPointerException ignore) {
                // Consumer was never started, processingThread is null - ignore
            } catch (Exception ignore) {
                // Ignore other cleanup errors
            }
        }
    }

    @BeforeAll
    void setUp() throws Exception {
        // Set up PostgreSQL
        embeddedPostgres = EmbeddedPostgres.builder().start();
        int port = embeddedPostgres.getPort();
        Connector postgresConnector = new PostgreSqlConnector(
                "localhost", port, "postgres", "postgres", "postgres");
        postgresPool = new ConnectionPool(postgresConnector, 2, 5);
        postgresProvider = new PoolConnectionProvider(postgresPool);
        postgresProviderName = "factory-test-postgres-" + System.currentTimeMillis();
        postgresProviderToken = ConnectionProvider.REGISTRY.bind(
                postgresProviderName, postgresProvider);

        // Set up SQLite
        sqliteDbFile = File.createTempFile("factory_test_", ".db");
        sqliteDbFile.deleteOnExit();
        Connector sqliteConnector = new SQLiteConnector(sqliteDbFile.getAbsolutePath());
        sqlitePool = new ConnectionPool(sqliteConnector, 2, 5);
        sqliteProvider = new PoolConnectionProvider(sqlitePool);
        sqliteProviderName = "factory-test-sqlite-" + System.currentTimeMillis();
        sqliteProviderToken = ConnectionProvider.REGISTRY.bind(
                sqliteProviderName, sqliteProvider);
    }

    @AfterAll
    void tearDown() throws Exception {
        // Clean up PostgreSQL
        if (postgresProviderToken != null && postgresProviderName != null) {
            try {
                ConnectionProvider.REGISTRY.unbind(postgresProviderName, postgresProviderToken);
            } catch (Exception ignore) {
                // ignore
            }
        }
        if (postgresPool != null) {
            postgresPool.shutdown();
        }
        if (embeddedPostgres != null) {
            embeddedPostgres.close();
        }

        // Clean up SQLite
        if (sqliteProviderToken != null && sqliteProviderName != null) {
            try {
                ConnectionProvider.REGISTRY.unbind(sqliteProviderName, sqliteProviderToken);
            } catch (Exception ignore) {
                // ignore
            }
        }
        if (sqlitePool != null) {
            sqlitePool.shutdown();
        }
        if (sqliteDbFile != null && sqliteDbFile.exists()) {
            sqliteDbFile.delete();
        }
    }

    // ========================================================================
    // DATABASE Consumer Tests
    // ========================================================================

    @Test
    @Order(100)
    void testGenerateDatabaseConsumerWithPostgreSQL() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, postgresProviderName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
        JsonObject config = builder.build();

        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.DATABASE, config);

        try {
            assertNotNull(consumer, "Consumer should not be null");
            assertInstanceOf(SQLConsumer.class, consumer,
                    "Consumer should be an instance of SQLConsumer");
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(200)
    void testGenerateDatabaseConsumerWithSQLite() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, sqliteProviderName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
        JsonObject config = builder.build();

        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.DATABASE, config);

        try {
            assertNotNull(consumer, "Consumer should not be null");
            assertInstanceOf(SQLConsumer.class, consumer,
                    "Consumer should be an instance of SQLConsumer");
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(300)
    void testGenerateDatabaseConsumerWithCustomParameters() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, sqliteProviderName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
        builder.add(SQLConsumer.MAXIMUM_RETRIES_KEY, 5);
        builder.add(SQLConsumer.RETRY_WAIT_TIME_KEY, 500);
        builder.add(SQLConsumer.LEASE_TIME_KEY, 60000);
        builder.add(SQLConsumer.MAXIMUM_LEASE_COUNT_KEY, 3);
        builder.add(SQLConsumer.MAXIMUM_SLEEP_TIME_KEY, 10000);
        JsonObject config = builder.build();

        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.DATABASE, config);

        try {
            assertNotNull(consumer, "Consumer should not be null");
            assertInstanceOf(SQLConsumer.class, consumer,
                    "Consumer should be an instance of SQLConsumer");
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(400)
    void testGenerateDatabaseConsumerMissingConnectionProvider() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        // Missing CONNECTION_PROVIDER_KEY
        JsonObject config = builder.build();

        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.DATABASE, config),
                "Should throw when connection provider is missing");
    }

    @Test
    @Order(500)
    void testGenerateDatabaseConsumerInvalidConnectionProvider() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, "non-existent-provider");
        JsonObject config = builder.build();

        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.DATABASE, config),
                "Should throw when connection provider does not exist");
    }

    // ========================================================================
    // RABBIT_MQ Consumer Tests
    // Note: RabbitMQ consumer uses lazy connection - it doesn't connect until
    // consume() is called, so init() succeeds even without a real server.
    // ========================================================================

    @Test
    @Order(600)
    void testGenerateRabbitMQConsumerType() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        // RabbitMQ consumer uses lazy connection - init() succeeds without server
        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.RABBIT_MQ, config);

        try {
            assertNotNull(consumer, "Consumer should not be null");
            assertInstanceOf(RabbitMQConsumer.class, consumer,
                    "Consumer should be an instance of RabbitMQConsumer");
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(700)
    void testGenerateRabbitMQConsumerMissingHost() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        // Missing MQ_HOST_KEY
        JsonObject config = builder.build();

        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.RABBIT_MQ, config),
                "Should throw when host is missing");
    }

    @Test
    @Order(800)
    void testGenerateRabbitMQConsumerMissingQueue() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        // Missing MQ_QUEUE_KEY
        JsonObject config = builder.build();

        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.RABBIT_MQ, config),
                "Should throw when queue is missing");
    }

    @Test
    @Order(900)
    void testGenerateRabbitMQConsumerWithAllParameters() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_PORT_KEY, 5672);
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "my-queue");
        builder.add(RabbitMQConsumer.MQ_VIRTUAL_HOST_KEY, "/vhost");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "admin");
        builder.add(RabbitMQConsumer.MQ_PASSWORD_KEY, "secret");
        JsonObject config = builder.build();

        // RabbitMQ consumer uses lazy connection - init() succeeds
        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.RABBIT_MQ, config);

        try {
            assertNotNull(consumer, "Consumer should not be null");
            assertInstanceOf(RabbitMQConsumer.class, consumer);
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(1000)
    void testGenerateRabbitMQConsumerUserWithoutPassword() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "admin");
        // Missing MQ_PASSWORD_KEY
        JsonObject config = builder.build();

        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.RABBIT_MQ, config),
                "Should throw when user is provided without password");
    }

    // ========================================================================
    // SQS Consumer Tests
    // Note: SQS consumer throws on init() when AWS region/credentials are not
    // configured. We verify the exception chain includes SQSConsumer.
    // ========================================================================

    /**
     * Helper method to check if a stack trace contains a frame from a specific class.
     */
    private boolean stackTraceContainsClass(Throwable t, Class<?> clazz) {
        String className = clazz.getName();
        for (StackTraceElement frame : t.getStackTrace()) {
            if (frame.getClassName().equals(className)) {
                return true;
            }
        }
        // Also check the cause chain
        if (t.getCause() != null && t.getCause() != t) {
            return stackTraceContainsClass(t.getCause(), clazz);
        }
        return false;
    }

    @Test
    @Order(1100)
    void testGenerateSQSConsumerType() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY,
                "https://sqs.us-east-1.amazonaws.com/123456789/test-queue");
        JsonObject config = builder.build();

        // SQS consumer throws on init() due to missing AWS region/credentials
        MessageConsumerSetupException exception = assertThrows(
                MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.SQS, config),
                "Should throw when unable to initialize SQS client");

        // Verify the exception originates from SQSConsumer
        assertTrue(stackTraceContainsClass(exception, SQSConsumer.class),
                "Exception stack trace should contain SQSConsumer class");

        // Verify there's a cause (AWS SDK exception)
        assertNotNull(exception.getCause(),
                "Exception should have a cause from AWS SDK");
    }

    @Test
    @Order(1200)
    void testGenerateSQSConsumerMissingUrl() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        // Missing SQS_URL_KEY
        JsonObject config = builder.build();

        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.SQS, config),
                "Should throw when SQS URL is missing");
    }

    @Test
    @Order(1300)
    void testGenerateSQSConsumerWithAllParameters() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY,
                "https://sqs.us-west-2.amazonaws.com/987654321/another-queue");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 10);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 1000);
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 120);
        JsonObject config = builder.build();

        // SQS consumer throws on init() due to missing AWS region/credentials
        MessageConsumerSetupException exception = assertThrows(
                MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.SQS, config),
                "Should throw when unable to initialize SQS client");

        // Verify the exception originates from SQSConsumer
        assertTrue(stackTraceContainsClass(exception, SQSConsumer.class),
                "Exception stack trace should contain SQSConsumer class");
    }

    // ========================================================================
    // Null and Edge Case Tests
    // ========================================================================

    @Test
    @Order(1400)
    void testGenerateMessageConsumerWithNullConfig() {
        // Test with DATABASE type and null config
        assertThrows(Exception.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.DATABASE, null),
                "Should throw when config is null");
    }

    @Test
    @Order(1500)
    void testGenerateMessageConsumerWithEmptyConfig() {
        JsonObject emptyConfig = Json.createObjectBuilder().build();

        // DATABASE requires connection provider
        assertThrows(MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.DATABASE, emptyConfig),
                "Should throw with empty config for DATABASE");
    }

    @Test
    @Order(1600)
    void testGenerateMessageConsumerWithNullConsumerType() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, sqliteProviderName);
        JsonObject config = builder.build();

        // Null consumer type results in NullPointerException from switch statement
        assertThrows(NullPointerException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(null, config),
                "Should throw NullPointerException when consumer type is null");
    }

    // ========================================================================
    // Consumer Type Verification Tests
    // ========================================================================

    @Test
    @Order(1700)
    void testConsumerTypeEnumValues() {
        // Verify all expected consumer types exist
        ConsumerType[] types = ConsumerType.values();
        assertEquals(3, types.length, "Should have 3 consumer types");

        // Verify each type
        assertNotNull(ConsumerType.valueOf("DATABASE"));
        assertNotNull(ConsumerType.valueOf("RABBIT_MQ"));
        assertNotNull(ConsumerType.valueOf("SQS"));
    }

    @Test
    @Order(1800)
    void testDatabaseConsumerReturnsSQLConsumerType() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, postgresProviderName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
        JsonObject config = builder.build();

        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.DATABASE, config);

        try {
            // Verify the exact class type
            assertEquals(SQLConsumer.class, consumer.getClass(),
                    "DATABASE type should return SQLConsumer class");
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(1900)
    void testRabbitMQConsumerReturnsCorrectType() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.RABBIT_MQ, config);

        try {
            assertEquals(RabbitMQConsumer.class, consumer.getClass(),
                    "RABBIT_MQ type should return RabbitMQConsumer class");
        } finally {
            safeDestroy(consumer);
        }
    }

    @Test
    @Order(2000)
    void testSQSConsumerThrowsWithSQSConsumerInStack() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY,
                "https://sqs.us-east-1.amazonaws.com/123456789/test-queue");
        JsonObject config = builder.build();

        // SQS consumer throws on init() - verify SQSConsumer is in the stack
        MessageConsumerSetupException exception = assertThrows(
                MessageConsumerSetupException.class,
                () -> MessageConsumerFactory.generateMessageConsumer(
                        ConsumerType.SQS, config));

        assertTrue(stackTraceContainsClass(exception, SQSConsumer.class),
                "SQS type should have SQSConsumer in exception stack trace");
    }

    // ========================================================================
    // MessageQueue Registry Test
    // ========================================================================

    @Test
    @Order(2100)
    void testDatabaseConsumerWithQueueRegistry() throws Exception {
        String queueRegistryName = "test-queue-registry-" + System.currentTimeMillis();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, sqliteProviderName);
        builder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
        builder.add(SQLConsumer.QUEUE_REGISTRY_NAME_KEY, queueRegistryName);
        JsonObject config = builder.build();

        MessageConsumer consumer = MessageConsumerFactory.generateMessageConsumer(
                ConsumerType.DATABASE, config);

        try {
            assertNotNull(consumer, "Consumer should not be null");
            assertInstanceOf(SQLConsumer.class, consumer);
        } finally {
            safeDestroy(consumer);
        }
    }

    // ========================================================================
    // Multiple Consumer Creation Tests
    // ========================================================================

    @Test
    @Order(2200)
    void testCreateMultipleDatabaseConsumers() throws Exception {
        MessageConsumer consumer1 = null;
        MessageConsumer consumer2 = null;

        try {
            // Create first consumer
            JsonObjectBuilder builder1 = Json.createObjectBuilder();
            builder1.add(SQLConsumer.CONNECTION_PROVIDER_KEY, postgresProviderName);
            builder1.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
            JsonObject config1 = builder1.build();

            consumer1 = MessageConsumerFactory.generateMessageConsumer(
                    ConsumerType.DATABASE, config1);

            // Create second consumer with different provider
            JsonObjectBuilder builder2 = Json.createObjectBuilder();
            builder2.add(SQLConsumer.CONNECTION_PROVIDER_KEY, sqliteProviderName);
            builder2.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
            JsonObject config2 = builder2.build();

            consumer2 = MessageConsumerFactory.generateMessageConsumer(
                    ConsumerType.DATABASE, config2);

            assertNotNull(consumer1, "First consumer should not be null");
            assertNotNull(consumer2, "Second consumer should not be null");
            assertNotSame(consumer1, consumer2,
                    "Each call should create a new consumer instance");
        } finally {
            safeDestroy(consumer1);
            safeDestroy(consumer2);
        }
    }

    @Test
    @Order(2300)
    void testCreateConsumersOfDifferentTypes() throws Exception {
        MessageConsumer dbConsumer = null;
        MessageConsumer mqConsumer = null;

        try {
            // Create DATABASE consumer
            JsonObjectBuilder dbBuilder = Json.createObjectBuilder();
            dbBuilder.add(SQLConsumer.CONNECTION_PROVIDER_KEY, sqliteProviderName);
            dbBuilder.add(SQLConsumer.CLEAN_DATABASE_KEY, true);
            dbConsumer = MessageConsumerFactory.generateMessageConsumer(
                    ConsumerType.DATABASE, dbBuilder.build());

            // Create RABBIT_MQ consumer (lazy connection, succeeds without server)
            JsonObjectBuilder mqBuilder = Json.createObjectBuilder();
            mqBuilder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
            mqBuilder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
            mqConsumer = MessageConsumerFactory.generateMessageConsumer(
                    ConsumerType.RABBIT_MQ, mqBuilder.build());

            // Verify they are different types
            assertInstanceOf(SQLConsumer.class, dbConsumer);
            assertInstanceOf(RabbitMQConsumer.class, mqConsumer);

            // Verify they are different instances
            assertNotSame(dbConsumer, mqConsumer);

            // Verify SQS throws on init (separate from other consumers)
            JsonObjectBuilder sqsBuilder = Json.createObjectBuilder();
            sqsBuilder.add(SQSConsumer.SQS_URL_KEY,
                    "https://sqs.us-east-1.amazonaws.com/123456789/test-queue");

            MessageConsumerSetupException sqsException = assertThrows(
                    MessageConsumerSetupException.class,
                    () -> MessageConsumerFactory.generateMessageConsumer(
                            ConsumerType.SQS, sqsBuilder.build()),
                    "SQS should throw on init without AWS credentials");

            assertTrue(stackTraceContainsClass(sqsException, SQSConsumer.class),
                    "SQS exception should contain SQSConsumer in stack");

        } finally {
            safeDestroy(dbConsumer);
            safeDestroy(mqConsumer);
        }
    }
}
