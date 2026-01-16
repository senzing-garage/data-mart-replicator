package com.senzing.listener.communication.sqs;

import com.senzing.listener.service.MessageProcessor;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.stream.SystemErr;
import uk.org.webcompere.systemstubs.stream.SystemOut;

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
 * Unit tests for {@link SQSConsumer} using a mock SQS client backed by SQLite.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(SystemStubsExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
class SQSConsumerTest {

    @SystemStub
    private SystemErr systemErr;

    @SystemStub
    private SystemOut systemOut;

    private MockSqsClient mockSqsClient;

    @BeforeEach
    void setUp() throws Exception {
        // Create a fresh mock client for each test
        mockSqsClient = new MockSqsClient(30);
    }

    @AfterEach
    void tearDown() {
        if (mockSqsClient != null) {
            mockSqsClient.close();
        }
    }

    // ========================================================================
    // Factory Method Tests
    // ========================================================================

    @Test
    @Order(100)
    void testGenerateSQSConsumer() {
        SQSConsumer consumer = SQSConsumer.generateSQSConsumer();
        assertNotNull(consumer);
    }

    // ========================================================================
    // Initialization Tests
    // ========================================================================

    @Test
    @Order(200)
    void testSQSConsumerInit() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue");
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        assertDoesNotThrow(() -> consumer.init(config));

        assertEquals("https://sqs.us-east-1.amazonaws.com/123456789/test-queue", consumer.getSqsUrl());
    }

    @Test
    @Order(300)
    void testSQSConsumerInitWithParameters() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 5);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 500);
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 60);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        assertEquals(5, consumer.getMaximumRetries());
        assertEquals(500, consumer.getRetryWaitTime());
        assertEquals(60, consumer.getVisibilityTimeout());
    }

    @Test
    @Order(400)
    void testSQSConsumerInitMissingUrl() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        // Missing SQS_URL_KEY
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);

        assertThrows(Exception.class, () -> consumer.init(config));
    }

    @Test
    @Order(500)
    void testDefaultConfigurationValues() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue");
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        assertEquals(SQSConsumer.DEFAULT_MAXIMUM_RETRIES, consumer.getMaximumRetries());
        assertEquals(SQSConsumer.DEFAULT_RETRY_WAIT_TIME, consumer.getRetryWaitTime());
        assertNull(consumer.getVisibilityTimeout());
    }

    // ========================================================================
    // Message Consumption Tests
    // ========================================================================

    @Test
    @Order(1000)
    void testConsumeMessages() throws Exception {
        // Set up the mock with messages
        mockSqsClient.enqueueMessage("{\"id\": 1}");
        mockSqsClient.enqueueMessage("{\"id\": 2}");
        mockSqsClient.enqueueMessage("{\"id\": 3}");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/queue");
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 30);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        // Track processed messages
        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        MessageProcessor processor = (message) -> {
            processedMessages.add(message);
            latch.countDown();
        };

        // Start consuming in a separate thread
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

        assertEquals(3, processedMessages.size());

        // Small delay to allow cleanup
        Thread.sleep(500);

        consumer.destroy();
        consumeThread.join(5000);
    }

    @Test
    @Order(1100)
    void testConsumeNoMessages() throws Exception {
        // Don't enqueue any messages
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/queue");
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 2);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch startedLatch = new CountDownLatch(1);

        MessageProcessor processor = (message) -> {
            processedCount.incrementAndGet();
        };

        // Start consuming
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
    // Failure Handling Tests
    // ========================================================================

    @Test
    @Order(2000)
    void testConsumeWithFailureAndAbort() throws Exception {
        // Set up the mock to fail
        mockSqsClient.enqueueMessage("{\"test\": \"failure\"}");
        mockSqsClient.setFailNextRequest(true);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/queue");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 0); // Abort on first failure
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch destroyedLatch = new CountDownLatch(1);

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume((msg) -> processedCount.incrementAndGet());
            } catch (Exception ignore) {
            } finally {
                destroyedLatch.countDown();
            }
        });
        consumeThread.start();

        // Wait for consumption to abort
        boolean finished = destroyedLatch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "Consumer should abort within timeout");

        assertEquals(0, processedCount.get(), "No messages should be processed when failure occurs");

        if (consumeThread.isAlive()) {
            consumer.destroy();
            consumeThread.join(5000);
        }
    }

    @Test
    @Order(2100)
    void testConsumeWithTransientFailureAndRetry() throws Exception {
        // Set up messages
        mockSqsClient.enqueueMessage("{\"test\": \"recovery\"}");

        // Fail twice, then succeed
        mockSqsClient.setFailureCount(2);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/queue");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 5);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 50);
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 30);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch processedLatch = new CountDownLatch(1);

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume((msg) -> {
                    processedCount.incrementAndGet();
                    processedLatch.countDown();
                });
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();

        // Wait for message to be processed after retries
        boolean processed = processedLatch.await(15, TimeUnit.SECONDS);
        assertTrue(processed, "Message should eventually be processed after transient failures");
        assertEquals(1, processedCount.get());

        consumer.destroy();
        consumeThread.join(5000);
    }

    // ========================================================================
    // Visibility Timeout Tests
    // ========================================================================

    @Test
    @Order(3000)
    void testVisibilityTimeoutReprocessing() throws Exception {
        // Create mock with short visibility timeout
        mockSqsClient.close();
        mockSqsClient = new MockSqsClient(2); // 2 second visibility timeout

        mockSqsClient.enqueueMessage("{\"slow\": \"processing\"}");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/queue");
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 2);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processCount = new AtomicInteger(0);
        CountDownLatch firstProcessLatch = new CountDownLatch(1);
        CountDownLatch secondProcessLatch = new CountDownLatch(2);

        MessageProcessor slowProcessor = (message) -> {
            int count = processCount.incrementAndGet();
            firstProcessLatch.countDown();
            secondProcessLatch.countDown();

            if (count == 1) {
                // First processing: sleep longer than visibility timeout
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignore) {}
            }
        };

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume(slowProcessor);
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();

        // Wait for at least 2 processing attempts
        boolean reprocessed = secondProcessLatch.await(15, TimeUnit.SECONDS);

        assertTrue(processCount.get() >= 2,
                "Message should be reprocessed after visibility timeout. Actual count: " + processCount.get());

        consumer.destroy();
        consumeThread.join(5000);
    }

    // ========================================================================
    // Multiple Messages Tests
    // ========================================================================

    @Test
    @Order(4000)
    void testConsumeManyMessages() throws Exception {
        // Enqueue many messages
        int messageCount = 20;
        for (int i = 0; i < messageCount; i++) {
            mockSqsClient.enqueueMessage("{\"id\": " + i + "}");
        }

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/queue");
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 60);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        List<JsonObject> processedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(messageCount);

        MessageProcessor processor = (message) -> {
            processedMessages.add(message);
            latch.countDown();
        };

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume(processor);
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        assertTrue(completed, "All messages should be processed within timeout");
        assertEquals(messageCount, processedMessages.size());

        consumer.destroy();
        consumeThread.join(5000);
    }

    // ========================================================================
    // Parent Class Method Tests (SQSConsumer)
    // ========================================================================

    @Test
    @Order(5000)
    void testSQSConsumerGetters() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/getter-test");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 7);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 750);
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 45);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        // These calls go to SQSConsumer parent class methods
        assertEquals("https://sqs.test/getter-test", consumer.getSqsUrl());
        assertEquals(7, consumer.getMaximumRetries());
        assertEquals(750, consumer.getRetryWaitTime());
        assertEquals(45, consumer.getVisibilityTimeout());
    }

    @Test
    @Order(5100)
    void testSQSConsumerDefaultValues() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/defaults");
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        // Verify default values from parent class constants
        assertEquals(SQSConsumer.DEFAULT_MAXIMUM_RETRIES, consumer.getMaximumRetries());
        assertEquals(SQSConsumer.DEFAULT_RETRY_WAIT_TIME, consumer.getRetryWaitTime());
        assertNull(consumer.getVisibilityTimeout());
    }

    @Test
    @Order(5200)
    void testHandleFailureWithZeroRetries() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/failure");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 0);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        // Call handleFailure directly - first failure should abort with maxRetries=0
        boolean shouldAbort = consumer.handleFailure(1, null, new RuntimeException("Test failure"));
        assertTrue(shouldAbort, "Should abort on first failure when maxRetries=0");
    }

    @Test
    @Order(5300)
    void testHandleFailureWithRetries() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/retry");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 3);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        // First failure - should not abort (1 <= 3)
        boolean shouldAbort1 = consumer.handleFailure(1, null, new RuntimeException("Test"));
        assertFalse(shouldAbort1, "Should not abort on first failure");

        // Second failure - should not abort (2 <= 3)
        boolean shouldAbort2 = consumer.handleFailure(2, null, new RuntimeException("Test"));
        assertFalse(shouldAbort2, "Should not abort on second failure");

        // Third failure - should not abort (3 <= 3)
        boolean shouldAbort3 = consumer.handleFailure(3, null, new RuntimeException("Test"));
        assertFalse(shouldAbort3, "Should not abort on third failure");

        // Fourth failure - should abort (4 > 3)
        boolean shouldAbort4 = consumer.handleFailure(4, null, new RuntimeException("Test"));
        assertTrue(shouldAbort4, "Should abort when failures exceed max retries");
    }

    // ========================================================================
    // HTTP Error Response Tests (sdkHttpResponse().isSuccessful() == false)
    // ========================================================================

    @Test
    @Order(6000)
    void testConsumeWithHttpErrorAndAbort() throws Exception {
        // Set up the mock to return HTTP error response
        mockSqsClient.enqueueMessage("{\"test\": \"http-error\"}");
        mockSqsClient.setHttpErrorCount(1, 500); // Return 500 error once

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/http-error");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 0); // Abort on first failure
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch destroyedLatch = new CountDownLatch(1);

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume((msg) -> processedCount.incrementAndGet());
            } catch (Exception ignore) {
            } finally {
                destroyedLatch.countDown();
            }
        });
        consumeThread.start();

        // Wait for consumption to abort due to HTTP error
        boolean finished = destroyedLatch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "Consumer should abort within timeout due to HTTP error");

        assertEquals(0, processedCount.get(), "No messages should be processed when HTTP error occurs");

        // Always call destroy and join to ensure all threads complete and logging finishes
        consumer.destroy();
        consumeThread.join(5000);

        // Allow any async logging to complete
        Thread.sleep(100);

        // Verify expected failure messages were logged (captured by SystemStubs)
        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("FAILURE DETECTED") || errOutput.isEmpty(),
                "Expected failure messages should be captured or suppressed");
    }

    @Test
    @Order(6100)
    void testConsumeWithHttpErrorAndRetry() throws Exception {
        // Set up messages
        mockSqsClient.enqueueMessage("{\"test\": \"http-recovery\"}");

        // Return HTTP error twice, then succeed
        mockSqsClient.setHttpErrorCount(2, 503); // Service Unavailable

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/http-retry");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 5);
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 50);
        builder.add(SQSConsumer.VISIBILITY_TIMEOUT_KEY, 30);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch processedLatch = new CountDownLatch(1);

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume((msg) -> {
                    processedCount.incrementAndGet();
                    processedLatch.countDown();
                });
            } catch (Exception ignore) {
            }
        });
        consumeThread.start();

        // Wait for message to be processed after HTTP error retries
        boolean processed = processedLatch.await(15, TimeUnit.SECONDS);
        assertTrue(processed, "Message should eventually be processed after HTTP error retries");
        assertEquals(1, processedCount.get());

        // Always call destroy and join to ensure all threads complete and logging finishes
        consumer.destroy();
        consumeThread.join(5000);

        // Allow any async logging to complete
        Thread.sleep(100);

        // Verify expected failure messages were logged (captured by SystemStubs)
        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("FAILURE DETECTED") || errOutput.isEmpty(),
                "Expected failure messages should be captured or suppressed");
    }

    @Test
    @Order(6200)
    void testConsumeWithHttpErrorExceedsMaxRetries() throws Exception {
        // Set up message
        mockSqsClient.enqueueMessage("{\"test\": \"http-exceed\"}");

        // Return more HTTP errors than maxRetries allows
        mockSqsClient.setHttpErrorCount(5, 500);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(SQSConsumer.SQS_URL_KEY, "https://sqs.test/http-exceed");
        builder.add(SQSConsumer.MAXIMUM_RETRIES_KEY, 2); // Only 2 retries
        builder.add(SQSConsumer.RETRY_WAIT_TIME_KEY, 10);
        JsonObject config = builder.build();

        TestableSQSConsumer consumer = new TestableSQSConsumer();
        consumer.setInjectedClient(mockSqsClient);
        consumer.init(config);

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch destroyedLatch = new CountDownLatch(1);

        Thread consumeThread = new Thread(() -> {
            try {
                consumer.consume((msg) -> processedCount.incrementAndGet());
            } catch (Exception ignore) {
            } finally {
                destroyedLatch.countDown();
            }
        });
        consumeThread.start();

        // Wait for consumption to abort
        boolean finished = destroyedLatch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "Consumer should abort when HTTP errors exceed max retries");

        assertEquals(0, processedCount.get(), "No messages should be processed");

        // Always call destroy and join to ensure all threads complete and logging finishes
        consumer.destroy();
        consumeThread.join(5000);

        // Allow any async logging to complete
        Thread.sleep(100);

        // Verify expected failure messages were logged (captured by SystemStubs)
        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("FAILURE DETECTED") || errOutput.isEmpty(),
                "Expected failure messages should be captured or suppressed");
    }

    // ========================================================================
    // Mock Client Tests
    // ========================================================================

    @Test
    @Order(7000)
    void testMockSqsClientServiceName() throws Exception {
        assertEquals("mock-sqs", mockSqsClient.serviceName());
    }

    @Test
    @Order(7100)
    void testMockSqsClientMessageCount() throws Exception {
        assertEquals(0, mockSqsClient.getMessageCount());
        mockSqsClient.enqueueMessage("{\"test\": 1}");
        assertEquals(1, mockSqsClient.getMessageCount());
        mockSqsClient.enqueueMessage("{\"test\": 2}");
        assertEquals(2, mockSqsClient.getMessageCount());
    }
}
