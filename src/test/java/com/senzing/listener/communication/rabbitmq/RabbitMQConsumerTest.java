package com.senzing.listener.communication.rabbitmq;

import com.senzing.listener.service.MessageProcessor;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RabbitMQConsumer} using a mock RabbitMQ connection backed by SQLite.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class RabbitMQConsumerTest {

    private MockConnectionFactory mockFactory;
    private MockRabbitMQChannel mockChannel;

    @BeforeEach
    void setUp() throws Exception {
        // Create a fresh mock factory and channel for each test
        mockChannel = new MockRabbitMQChannel(30);
        mockFactory = new MockConnectionFactory(mockChannel);
    }

    @AfterEach
    void tearDown() {
        if (mockFactory != null && mockFactory.getMockConnection() != null) {
            try {
                mockFactory.getMockConnection().close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    // ========================================================================
    // Initialization Tests
    // ========================================================================

    @Test
    @Order(100)
    void testRabbitMQConsumerInit() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        assertDoesNotThrow(() -> consumer.init(config));

        assertEquals("test-queue", consumer.getQueueName());
    }

    @Test
    @Order(200)
    void testRabbitMQConsumerInitWithAllParameters() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "rabbitmq.example.com");
        builder.add(RabbitMQConsumer.MQ_PORT_KEY, 5673);
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "my-queue");
        builder.add(RabbitMQConsumer.MQ_VIRTUAL_HOST_KEY, "/vhost");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "admin");
        builder.add(RabbitMQConsumer.MQ_PASSWORD_KEY, "secret");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        assertDoesNotThrow(() -> consumer.init(config));

        assertEquals("my-queue", consumer.getQueueName());
    }

    @Test
    @Order(300)
    void testRabbitMQConsumerInitMissingHost() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        // Missing MQ_HOST_KEY
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);

        assertThrows(Exception.class, () -> consumer.init(config));
    }

    @Test
    @Order(400)
    void testRabbitMQConsumerInitMissingQueue() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        // Missing MQ_QUEUE_KEY
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);

        assertThrows(Exception.class, () -> consumer.init(config));
    }

    @Test
    @Order(500)
    void testRabbitMQConsumerInitUserWithoutPassword() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "admin");
        // Missing MQ_PASSWORD_KEY
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);

        assertThrows(Exception.class, () -> consumer.init(config));
    }

    @Test
    @Order(600)
    void testRabbitMQConsumerInitPasswordWithoutUser() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        // Missing MQ_USER_KEY
        builder.add(RabbitMQConsumer.MQ_PASSWORD_KEY, "secret");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);

        assertThrows(Exception.class, () -> consumer.init(config));
    }

    // ========================================================================
    // Message Consumption Tests
    // ========================================================================

    @Test
    @Order(1000)
    void testConsumeMessages() throws Exception {
        // Set up the mock with messages
        mockChannel.enqueueMessage("{\"id\": 1}");
        mockChannel.enqueueMessage("{\"id\": 2}");
        mockChannel.enqueueMessage("{\"id\": 3}");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        // Track processed messages
        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        MessageProcessor processor = (message) -> {
            processedMessages.add(message);
            latch.countDown();
        };

        // Start consuming
        consumer.consume(processor);

        // Wait for messages to be processed
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");

        assertEquals(3, processedMessages.size());

        // Small delay to allow cleanup
        Thread.sleep(500);

        consumer.destroy();
    }

    @Test
    @Order(1100)
    void testConsumeNoMessages() throws Exception {
        // Don't enqueue any messages
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch startedLatch = new CountDownLatch(1);

        MessageProcessor processor = (message) -> {
            processedCount.incrementAndGet();
        };

        // Start consuming in a separate thread
        Thread consumeThread = new Thread(() -> {
            startedLatch.countDown();
            try {
                consumer.consume(processor);
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();

        // Wait for consumer to start
        startedLatch.await(5, TimeUnit.SECONDS);
        Thread.sleep(1000); // Let it poll once

        // Destroy the consumer
        consumer.destroy();
        consumeThread.join(5000);

        assertEquals(0, processedCount.get(), "No messages should be processed");
    }

    // ========================================================================
    // Multiple Messages Tests
    // ========================================================================

    @Test
    @Order(2000)
    void testConsumeManyMessages() throws Exception {
        // Enqueue many messages
        int messageCount = 20;
        for (int i = 0; i < messageCount; i++) {
            mockChannel.enqueueMessage("{\"id\": " + i + "}");
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(messageCount);

        MessageProcessor processor = (message) -> {
            processedMessages.add(message);
            latch.countDown();
        };

        // Start consuming
        consumer.consume(processor);

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");
        assertEquals(messageCount, processedMessages.size());

        consumer.destroy();
    }

    @Test
    @Order(2100)
    void testConsumeMessagesWithProcessingDelay() throws Exception {
        // Set up messages
        mockChannel.enqueueMessage("{\"id\": 1}");
        mockChannel.enqueueMessage("{\"id\": 2}");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(2);

        MessageProcessor processor = (message) -> {
            try {
                Thread.sleep(100); // Simulate processing delay
            } catch (InterruptedException ignore) {}
            processedMessages.add(message);
            latch.countDown();
        };

        // Start consuming
        consumer.consume(processor);

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");
        assertEquals(2, processedMessages.size());

        consumer.destroy();
    }

    // ========================================================================
    // Generate Factory Method Tests
    // ========================================================================

    @Test
    @Order(3000)
    void testGenerateRabbitMQConsumer() {
        RabbitMQConsumer consumer = RabbitMQConsumer.generateRabbitMQConsumer();
        assertNotNull(consumer);
    }

    // ========================================================================
    // ConnectionFactory Configuration Tests
    // ========================================================================

    @Test
    @Order(3500)
    void testConnectionFactoryConfigurationMinimal() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "test-host.example.com");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        // Start consuming to trigger createConnection()
        CountDownLatch startedLatch = new CountDownLatch(1);
        Thread consumeThread = new Thread(() -> {
            startedLatch.countDown();
            try {
                consumer.consume((msg) -> {});
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();
        startedLatch.await(5, TimeUnit.SECONDS);
        Thread.sleep(500);

        // Verify factory was configured correctly
        assertEquals("test-host.example.com", mockFactory.getConfiguredHost());
        assertNull(mockFactory.getConfiguredPort(), "Port should not be set");
        assertNull(mockFactory.getConfiguredVirtualHost(), "Virtual host should not be set");
        assertNull(mockFactory.getConfiguredUsername(), "Username should not be set");
        assertNull(mockFactory.getConfiguredPassword(), "Password should not be set");
        assertEquals(1, mockFactory.getNewConnectionCallCount());

        consumer.destroy();
        consumeThread.join(5000);
    }

    @Test
    @Order(3600)
    void testConnectionFactoryConfigurationFull() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "rabbitmq.example.com");
        builder.add(RabbitMQConsumer.MQ_PORT_KEY, 5673);
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "my-queue");
        builder.add(RabbitMQConsumer.MQ_VIRTUAL_HOST_KEY, "/production");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "rabbit_user");
        builder.add(RabbitMQConsumer.MQ_PASSWORD_KEY, "rabbit_pass");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        // Start consuming to trigger createConnection()
        CountDownLatch startedLatch = new CountDownLatch(1);
        Thread consumeThread = new Thread(() -> {
            startedLatch.countDown();
            try {
                consumer.consume((msg) -> {});
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();
        startedLatch.await(5, TimeUnit.SECONDS);
        Thread.sleep(500);

        // Verify factory was configured with all parameters
        assertEquals("rabbitmq.example.com", mockFactory.getConfiguredHost());
        assertEquals(5673, mockFactory.getConfiguredPort());
        assertEquals("/production", mockFactory.getConfiguredVirtualHost());
        assertEquals("rabbit_user", mockFactory.getConfiguredUsername());
        assertEquals("rabbit_pass", mockFactory.getConfiguredPassword());
        assertEquals(1, mockFactory.getNewConnectionCallCount());

        consumer.destroy();
        consumeThread.join(5000);
    }

    // ========================================================================
    // Channel Message Count Test
    // ========================================================================

    @Test
    @Order(4000)
    void testMockChannelMessageCount() throws Exception {
        assertEquals(0, mockChannel.getMessageCount());

        mockChannel.enqueueMessage("{\"test\": 1}");
        assertEquals(1, mockChannel.getMessageCount());

        mockChannel.enqueueMessage("{\"test\": 2}");
        assertEquals(2, mockChannel.getMessageCount());
    }

    @Test
    @Order(4100)
    void testMockChannelBasicPublish() throws Exception {
        assertEquals(0, mockChannel.getMessageCount());

        // Use basicPublish to add a message
        mockChannel.basicPublish("", "test-queue", null, "{\"published\": true}".getBytes());
        assertEquals(1, mockChannel.getMessageCount());
    }

    // ========================================================================
    // Connection Tests
    // ========================================================================

    @Test
    @Order(5000)
    void testMockConnectionCreateChannel() throws Exception {
        MockRabbitMQConnection mockConnection = mockFactory.getMockConnection();
        assertNotNull(mockConnection.createChannel());
        assertEquals(mockChannel, mockConnection.createChannel());
    }

    @Test
    @Order(5100)
    void testMockConnectionIsOpen() throws Exception {
        MockRabbitMQConnection mockConnection = mockFactory.getMockConnection();
        assertTrue(mockConnection.isOpen());
        mockConnection.close();
        assertFalse(mockConnection.isOpen());
    }

    @Test
    @Order(5200)
    void testMockConnectionProperties() throws Exception {
        MockRabbitMQConnection mockConnection = mockFactory.getMockConnection();
        assertEquals(5672, mockConnection.getPort());
        assertEquals("mock-connection", mockConnection.getClientProvidedName());
        assertEquals("mock-connection-id", mockConnection.getId());
        assertNotNull(mockConnection.getAddress());
    }

    // ========================================================================
    // Channel State Tests
    // ========================================================================

    @Test
    @Order(6000)
    void testMockChannelIsOpen() throws Exception {
        MockRabbitMQChannel channel = new MockRabbitMQChannel();
        assertTrue(channel.isOpen());
        channel.close();
        assertFalse(channel.isOpen());
    }

    @Test
    @Order(6100)
    void testMockChannelQueueDeclare() throws Exception {
        var result = mockChannel.queueDeclare("test-queue", true, false, false, null);
        assertNotNull(result);
        assertEquals("test-queue", result.getQueue());
    }

    @Test
    @Order(6200)
    void testMockChannelExchangeDeclare() throws Exception {
        var result = mockChannel.exchangeDeclare("test-exchange", "direct");
        assertNotNull(result);
    }

    @Test
    @Order(6300)
    void testMockChannelTransactions() throws Exception {
        assertNotNull(mockChannel.txSelect());
        assertNotNull(mockChannel.txCommit());
        assertNotNull(mockChannel.txRollback());
    }

    @Test
    @Order(6400)
    void testMockChannelConfirm() throws Exception {
        assertNotNull(mockChannel.confirmSelect());
        assertTrue(mockChannel.waitForConfirms());
        assertTrue(mockChannel.waitForConfirms(1000));
    }

    // ========================================================================
    // Empty/Blank User/Password Tests
    // ========================================================================

    @Test
    @Order(7000)
    void testRabbitMQConsumerInitEmptyUser() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "");
        builder.add(RabbitMQConsumer.MQ_PASSWORD_KEY, "");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        // Empty user/password should be treated as not provided
        assertDoesNotThrow(() -> consumer.init(config));
    }

    @Test
    @Order(7100)
    void testRabbitMQConsumerInitBlankUser() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        builder.add(RabbitMQConsumer.MQ_USER_KEY, "   ");
        builder.add(RabbitMQConsumer.MQ_PASSWORD_KEY, "   ");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        // Blank user/password should be treated as not provided
        assertDoesNotThrow(() -> consumer.init(config));
    }

    // ========================================================================
    // Throttle Consumption Tests
    // ========================================================================

    @Test
    @Order(8000)
    void testThrottleConsumptionTriggered() throws Exception {
        // Enqueue many messages to trigger throttling
        for (int i = 0; i < 20; i++) {
            mockChannel.enqueueMessage("{\"id\": " + i + "}");
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.setMaximumPendingCount(5); // Low threshold to trigger throttling
        consumer.init(config);

        CountDownLatch latch = new CountDownLatch(20);
        AtomicInteger processedCount = new AtomicInteger(0);

        MessageProcessor processor = (message) -> {
            try {
                Thread.sleep(200); // Slow processing to allow queue to build up
            } catch (InterruptedException ignore) {}
            processedCount.incrementAndGet();
            latch.countDown();
        };

        consumer.consume(processor);
        boolean completed = latch.await(60, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");
        assertEquals(20, processedCount.get());

        // Verify basicCancel was called (throttling occurred)
        // Note: basicCancel is also called by destroy(), so we check > 1
        int cancelCount = mockChannel.getBasicCancelCallCount();
        assertTrue(cancelCount >= 1, "basicCancel should be called during throttling or cleanup");

        consumer.destroy();
    }

    @Test
    @Order(8100)
    void testThrottleConsumptionWithVerySlowProcessor() throws Exception {
        // Enqueue messages
        for (int i = 0; i < 10; i++) {
            mockChannel.enqueueMessage("{\"id\": " + i + "}");
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.setMaximumPendingCount(3); // Very low threshold
        consumer.init(config);

        CountDownLatch latch = new CountDownLatch(10);
        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());

        MessageProcessor processor = (message) -> {
            try {
                Thread.sleep(300); // Very slow processing
            } catch (InterruptedException ignore) {}
            processedMessages.add(message);
            latch.countDown();
        };

        consumer.consume(processor);
        boolean completed = latch.await(90, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");
        assertEquals(10, processedMessages.size());

        consumer.destroy();
    }

    // ========================================================================
    // Exception Injection Tests
    // ========================================================================

    @Test
    @Order(9000)
    void testBasicConsumeIOExceptionInDoConsume() throws Exception {
        // Set up channel to throw on basicConsume
        mockChannel.setThrowOnBasicConsume(true);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        // Suppress console output from exception logging
        new SystemErr().execute(() -> {
            // Consume should throw MessageConsumerSetupException wrapping the IOException
            assertThrows(Exception.class, () -> consumer.consume((msg) -> {}));
        });
    }

    @Test
    @Order(9100)
    void testBasicCancelIOExceptionInThrottleConsumption() throws Exception {
        // Enqueue messages to trigger throttling
        for (int i = 0; i < 15; i++) {
            mockChannel.enqueueMessage("{\"id\": " + i + "}");
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.setMaximumPendingCount(5);
        consumer.init(config);

        CountDownLatch latch = new CountDownLatch(10);
        AtomicInteger processedCount = new AtomicInteger(0);

        MessageProcessor processor = (message) -> {
            int count = processedCount.incrementAndGet();
            // After a few messages, enable basicCancel exception
            if (count == 3) {
                mockChannel.setThrowOnBasicCancel(true);
            }
            try {
                Thread.sleep(150);
            } catch (InterruptedException ignore) {}
            latch.countDown();
        };

        // Suppress console output from exception logging in consumer thread
        new SystemErr().execute(() -> {
            // Start consuming
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume(processor);
                } catch (Exception ignore) {
                }
            });
            consumeThread.start();

            // Wait for some messages to be processed (exception should be logged but not stop processing)
            latch.await(60, TimeUnit.SECONDS);

            // Disable the exception for cleanup
            mockChannel.setThrowOnBasicCancel(false);
            consumer.destroy();
            consumeThread.join(5000);
        });

        // Verify that processing continued despite the exception
        assertTrue(processedCount.get() >= 10, "Should have processed messages despite basicCancel exception");
    }

    @Test
    @Order(9200)
    void testBasicAckIOExceptionInDisposeMessage() throws Exception {
        // Enqueue a message
        mockChannel.enqueueMessage("{\"id\": 1}");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        // Enable basicAck exception
        mockChannel.setThrowOnBasicAck(true);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger processedCount = new AtomicInteger(0);

        MessageProcessor processor = (message) -> {
            processedCount.incrementAndGet();
            latch.countDown();
        };

        // Suppress console output from exception logging in consumer thread
        new SystemErr().execute(() -> {
            // Start consuming
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume(processor);
                } catch (Exception ignore) {
                }
            });
            consumeThread.start();

            // Wait for the message to be processed
            latch.await(30, TimeUnit.SECONDS);

            // Give time for disposeMessage to be called (which will log but not throw)
            Thread.sleep(500);

            mockChannel.setThrowOnBasicAck(false);
            consumer.destroy();
            consumeThread.join(5000);
        });

        // Message should have been processed (exception only logged)
        assertEquals(1, processedCount.get());
    }

    @Test
    @Order(9300)
    void testBasicCancelIOExceptionInDoDestroy() throws Exception {
        // Enqueue a message
        mockChannel.enqueueMessage("{\"id\": 1}");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        CountDownLatch latch = new CountDownLatch(1);

        MessageProcessor processor = (message) -> {
            latch.countDown();
        };

        // Suppress console output from exception logging in consumer thread
        new SystemErr().execute(() -> {
            // Start consuming
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume(processor);
                } catch (Exception ignore) {
                }
            });
            consumeThread.start();

            // Wait for the message to be processed
            latch.await(30, TimeUnit.SECONDS);
            Thread.sleep(500);

            // Enable basicCancel exception before destroy
            mockChannel.setThrowOnBasicCancel(true);

            // Destroy should handle the exception gracefully (log warning but not throw)
            assertDoesNotThrow(() -> consumer.destroy());
            consumeThread.join(5000);
        });
    }

    @Test
    @Order(9400)
    void testQueueDeclareRetryOnFirstFailure() throws Exception {
        // Configure channel to fail on first queueDeclare, succeed on second
        mockChannel.setThrowOnQueueDeclareCount(1);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.init(config);

        // Suppress console output from exception logging during retry
        new SystemErr().execute(() -> {
            // Start consuming - getChannel should retry after first failure
            CountDownLatch startedLatch = new CountDownLatch(1);
            Thread consumeThread = new Thread(() -> {
                startedLatch.countDown();
                try {
                    consumer.consume((msg) -> {});
                } catch (Exception ignore) {
                }
            });
            consumeThread.start();
            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(500);

            // Verify queueDeclare was called twice (first failed, second succeeded)
            assertEquals(2, mockChannel.getQueueDeclareCallCount(),
                    "queueDeclare should be called twice due to retry");

            consumer.destroy();
            consumeThread.join(5000);
        });
    }

    @Test
    @Order(9500)
    void testBasicConsumeIOExceptionInThrottleResumeThread() throws Exception {
        // Enqueue messages to trigger throttling
        for (int i = 0; i < 20; i++) {
            mockChannel.enqueueMessage("{\"id\": " + i + "}");
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(RabbitMQConsumer.MQ_HOST_KEY, "localhost");
        builder.add(RabbitMQConsumer.MQ_QUEUE_KEY, "test-queue");
        JsonObject config = builder.build();

        TestableRabbitMQConsumer consumer = new TestableRabbitMQConsumer();
        consumer.setInjectedConnectionFactory(mockFactory);
        consumer.setMaximumPendingCount(5);
        consumer.init(config);

        CountDownLatch processedLatch = new CountDownLatch(5);
        AtomicInteger processedCount = new AtomicInteger(0);

        MessageProcessor processor = (message) -> {
            int count = processedCount.incrementAndGet();
            // After initial processing triggers throttle, set up for resume failure
            if (count == 5) {
                // Next basicConsume (in the throttle resume thread) will fail
                mockChannel.setThrowOnBasicConsume(true);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignore) {}
            processedLatch.countDown();
        };

        // Suppress console output from exception logging in consumer thread
        new SystemErr().execute(() -> {
            // Start consuming
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume(processor);
                } catch (Exception ignore) {
                }
            });
            consumeThread.start();

            // Wait for initial messages and throttle to trigger
            processedLatch.await(60, TimeUnit.SECONDS);
            Thread.sleep(2000); // Allow throttle resume thread to run and fail

            mockChannel.setThrowOnBasicConsume(false);
            consumer.destroy();
            consumeThread.join(5000);
        });

        // Should have processed some messages before the throttle resume failure
        assertTrue(processedCount.get() >= 5, "Should have processed initial messages");
    }
}
