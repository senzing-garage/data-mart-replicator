## Consumer Types and Message Batching

### What are the three message consumer types?

**SQSConsumer** (Amazon SQS)
- Polls SQS queue for messages using the AWS SDK
- Supports long polling for efficiency
- Messages deleted from SQS only after the entire batch is successfully processed
- Tracks consecutive failure count; aborts if exceeding `MAXIMUM_RETRIES` (default 0)
- Waits `RETRY_WAIT_TIME` (default 1000ms) between retries
- Special: overrides `waitUntilDestroyed()` to prevent deadlock when destroying from the consumer thread

**RabbitMQConsumer** (RabbitMQ/AMQP)
- Uses push-based delivery (RabbitMQ pushes messages to the client)
- Messages acknowledged only after batch completion
- Throttling requires cancelling the consumer subscription entirely (can't pause push delivery)
- On backpressure: cancels subscription, waits for drain in background thread, resubscribes
- Binds to a configured exchange with a queue

**SQLConsumer** (SQL-based queue)
- Reads from `sz_message_queue` table in the data mart database itself
- Uses lease-based consumption for multi-instance safety
- Progressive backoff on empty queue: 1s sleep, doubling to max 10s
- Overrides `waitUntilDestroyed()` with deadlock-safe thread join tracking
- Useful for deployments without external message infrastructure

### How does message batching work?

A single incoming message (from SQS, RabbitMQ, or SQL) may contain:
- A single JSON object (one INFO message)
- A JSON array of objects (multiple INFO messages)

`AbstractMessageConsumer.enqueueMessages()` unpacks these into individual `InfoMessage` objects within a `MessageBatch`. The batch:
- Tracks `pendingCount` (how many INFO messages remain unprocessed)
- Tracks whether any message `failed`
- Is only "disposable" when all messages are processed AND none failed
- Disposition means the original queue message can be acknowledged/deleted

If ANY message in a batch fails, the ENTIRE batch is not disposed — meaning the original message stays in the queue for redelivery. This provides at-least-once semantics at the batch level.

### Consumer selection

The consumer type is determined by which command-line options are provided to `SzReplicator`:
- SQS URL provided → `SQSConsumer`
- RabbitMQ URL provided → `RabbitMQConsumer`
- Neither → `SQLConsumer` (reads from the data mart database)

`MessageConsumerFactory` creates the appropriate implementation.

### The SQSConsumer deadlock fix

A known issue: if `destroy()` is called from the consumption thread itself (e.g., after max retries exceeded), `waitUntilDestroyed()` would deadlock — the thread waits for itself to finish. The fix overrides `waitUntilDestroyed()` to short-circuit when the calling thread IS the consumption thread. The same pattern exists in `SQLConsumer`.
