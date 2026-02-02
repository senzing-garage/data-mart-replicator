package com.senzing.listener.communication.sqs;

import com.senzing.listener.communication.sql.LeasedMessage;
import com.senzing.listener.communication.sql.SQLiteClient;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.Connector;
import com.senzing.sql.SQLiteConnector;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mock implementation of {@link SqsClient} that uses SQLite as the backing
 * message queue storage. This allows testing {@link SQSConsumer} without
 * requiring an actual AWS SQS connection.
 */
public class MockSqsClient implements SqsClient {

    private final ConnectionPool connectionPool;
    private final SQLiteClient sqlClient;
    private final File tempDbFile;
    private final int visibilityTimeoutSeconds;
    private final AtomicLong receiptHandleCounter = new AtomicLong(0);

    // Maps receipt handles to message IDs for deletion
    private final Map<String, Long> receiptHandleToMessageId = new ConcurrentHashMap<>();

    // Tracks which messages are currently "invisible" (leased)
    private final Map<Long, Long> messageVisibilityExpiration = new ConcurrentHashMap<>();

    private volatile boolean closed = false;
    private volatile boolean failNextRequest = false;
    private volatile int failureCount = 0;
    private volatile int httpErrorCount = 0;
    private volatile int httpErrorStatusCode = 500;

    /**
     * Creates a new MockSqsClient with default visibility timeout of 30 seconds.
     */
    public MockSqsClient() throws Exception {
        this(30);
    }

    /**
     * Creates a new MockSqsClient with the specified visibility timeout.
     *
     * @param visibilityTimeoutSeconds The visibility timeout in seconds.
     */
    public MockSqsClient(int visibilityTimeoutSeconds) throws Exception {
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;

        // Create a temporary SQLite database
        this.tempDbFile = File.createTempFile("mock_sqs_", ".db");
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

    /**
     * Configures the mock to fail the next request.
     */
    public void setFailNextRequest(boolean fail) {
        this.failNextRequest = fail;
    }

    /**
     * Configures the mock to fail a specified number of requests.
     */
    public void setFailureCount(int count) {
        this.failureCount = count;
    }

    /**
     * Configures the mock to return unsuccessful HTTP responses for a specified
     * number of requests. Unlike setFailureCount which throws an exception,
     * this returns a response with an unsuccessful HTTP status code.
     *
     * @param count The number of requests to return HTTP errors for.
     * @param statusCode The HTTP status code to return (e.g., 500, 503).
     */
    public void setHttpErrorCount(int count, int statusCode) {
        this.httpErrorCount = count;
        this.httpErrorStatusCode = statusCode;
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request)
            throws AwsServiceException, SdkClientException {

        // Check for simulated failures FIRST (test's configured behavior takes precedence)
        if (failNextRequest || failureCount > 0) {
            if (failureCount > 0) {
                failureCount--;
            }
            failNextRequest = false;

            // Throw an exception to simulate SQS failure
            throw SdkClientException.create("Simulated SQS failure");
        }

        // Check for simulated HTTP error responses (test's configured behavior)
        if (httpErrorCount > 0) {
            httpErrorCount--;

            // Return a response with unsuccessful HTTP status
            SdkHttpResponse httpResponse = SdkHttpResponse.builder()
                    .statusCode(httpErrorStatusCode)
                    .statusText("Simulated HTTP Error")
                    .build();

            return (ReceiveMessageResponse) ((SdkResponse.Builder) ReceiveMessageResponse.builder()
                    .messages(Collections.emptyList()))
                    .sdkHttpResponse(httpResponse)
                    .build();
        }

        // Check if client is closed (after test's configured errors are exhausted)
        if (closed) {
            // Return an unsuccessful response instead of throwing, allowing
            // the consumer's while loop to check state and exit gracefully
            SdkHttpResponse httpResponse = SdkHttpResponse.builder()
                    .statusCode(503)
                    .statusText("Client closed")
                    .build();
            return (ReceiveMessageResponse) ((SdkResponse.Builder) ReceiveMessageResponse.builder()
                    .messages(Collections.emptyList()))
                    .sdkHttpResponse(httpResponse)
                    .build();
        }

        try (Connection conn = connectionPool.acquire()) {
            conn.setAutoCommit(false);

            // Release any expired visibility timeouts
            releaseExpiredMessages();

            // Get visibility timeout from request or use default
            Integer timeout = request.visibilityTimeout();
            int leaseTime = (timeout != null) ? timeout : visibilityTimeoutSeconds;

            // Generate a unique lease ID
            String leaseId = "mock-lease-" + System.currentTimeMillis() + "-" +
                    receiptHandleCounter.incrementAndGet();

            // Lease messages
            int maxMessages = request.maxNumberOfMessages() != null ?
                    request.maxNumberOfMessages() : 10;
            sqlClient.leaseMessages(conn, leaseId, leaseTime, maxMessages);
            conn.commit();

            // Get leased messages
            List<LeasedMessage> leasedMessages = sqlClient.getLeasedMessages(conn, leaseId);

            // Convert to SQS Message objects
            List<Message> sqsMessages = new ArrayList<>();
            for (LeasedMessage lm : leasedMessages) {
                String receiptHandle = "receipt-" + receiptHandleCounter.incrementAndGet();
                receiptHandleToMessageId.put(receiptHandle, lm.getMessageId());
                messageVisibilityExpiration.put(lm.getMessageId(), lm.getLeaseExpiration());

                Message sqsMessage = Message.builder()
                        .messageId(String.valueOf(lm.getMessageId()))
                        .body(lm.getMessageText())
                        .receiptHandle(receiptHandle)
                        .build();
                sqsMessages.add(sqsMessage);
            }

            // Build response with successful HTTP status
            SdkHttpResponse httpResponse = SdkHttpResponse.builder()
                    .statusCode(200)
                    .statusText("OK")
                    .build();

            return (ReceiveMessageResponse) ((SdkResponse.Builder) ReceiveMessageResponse.builder()
                    .messages(sqsMessages))
                    .sdkHttpResponse(httpResponse)
                    .build();

        } catch (SQLException e) {
            throw SdkClientException.create("Database error: " + e.getMessage(), e);
        }
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request)
            throws AwsServiceException, SdkClientException {

        if (closed) {
            throw SdkClientException.create("Client is closed");
        }

        String receiptHandle = request.receiptHandle();
        Long messageId = receiptHandleToMessageId.remove(receiptHandle);

        // Build HTTP response for success
        SdkHttpResponse httpResponse = SdkHttpResponse.builder()
                .statusCode(200)
                .statusText("OK")
                .build();

        if (messageId == null) {
            // Message already deleted or invalid receipt handle
            return (DeleteMessageResponse) ((SdkResponse.Builder) DeleteMessageResponse.builder())
                    .sdkHttpResponse(httpResponse)
                    .build();
        }

        try (Connection conn = connectionPool.acquire()) {
            conn.setAutoCommit(false);

            // Delete the message - we use a simplified approach since we have the message ID
            // In real SQS, the receipt handle would be validated
            sqlClient.deleteMessage(conn, messageId, null);
            conn.commit();

            messageVisibilityExpiration.remove(messageId);

            return (DeleteMessageResponse) ((SdkResponse.Builder) DeleteMessageResponse.builder())
                    .sdkHttpResponse(httpResponse)
                    .build();

        } catch (SQLException e) {
            throw SdkClientException.create("Database error: " + e.getMessage(), e);
        }
    }

    /**
     * Releases messages whose visibility timeout has expired.
     */
    private void releaseExpiredMessages() {
        long now = System.currentTimeMillis();
        try (Connection conn = connectionPool.acquire()) {
            conn.setAutoCommit(false);
            sqlClient.releaseExpiredLeases(conn, visibilityTimeoutSeconds);
            conn.commit();
        } catch (SQLException e) {
            // Log but don't fail
        }

        // Clean up expired entries from our tracking maps
        messageVisibilityExpiration.entrySet().removeIf(entry -> entry.getValue() < now);
    }

    @Override
    public String serviceName() {
        return "mock-sqs";
    }

    @Override
    public void close() {
        this.closed = true;
        if (connectionPool != null) {
            connectionPool.shutdown();
        }
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    // ========================================================================
    // Unsupported operations - throw UnsupportedOperationException
    // ========================================================================

    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        throw new java.lang.UnsupportedOperationException("Use enqueueMessage() instead");
    }

    @Override
    public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public GetQueueUrlResponse getQueueUrl(GetQueueUrlRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public ListQueuesResponse listQueues(ListQueuesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public PurgeQueueResponse purgeQueue(PurgeQueueRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public SetQueueAttributesResponse setQueueAttributes(SetQueueAttributesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public AddPermissionResponse addPermission(AddPermissionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public RemovePermissionResponse removePermission(RemovePermissionRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public TagQueueResponse tagQueue(TagQueueRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public UntagQueueResponse untagQueue(UntagQueueRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public ListQueueTagsResponse listQueueTags(ListQueueTagsRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public ListDeadLetterSourceQueuesResponse listDeadLetterSourceQueues(ListDeadLetterSourceQueuesRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public ListMessageMoveTasksResponse listMessageMoveTasks(ListMessageMoveTasksRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public StartMessageMoveTaskResponse startMessageMoveTask(StartMessageMoveTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }

    @Override
    public CancelMessageMoveTaskResponse cancelMessageMoveTask(CancelMessageMoveTaskRequest request) {
        throw new java.lang.UnsupportedOperationException("Not implemented in mock");
    }
}
