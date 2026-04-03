## Backpressure and Throttling

### How does the replicator prevent memory exhaustion under high message load?

**Backpressure Mechanism (AbstractMessageConsumer)**

The consumer tracks a `pendingMessages` queue (LinkedList). When the queue size reaches the maximum (`concurrency * 1000` messages), consumption is throttled:

1. `throttleConsumption()` is called — pauses pulling new messages from the queue
2. The consumer waits until pending count drops to half the maximum (`concurrency * 500`)
3. Consumption resumes

This prevents unbounded memory growth when messages arrive faster than they can be processed.

**Consumer-Specific Throttling Behavior:**

- **SQSConsumer**: Stops polling SQS. Resumes when backpressure clears.
- **RabbitMQConsumer**: Cancels the RabbitMQ consumer subscription entirely. Spawns a background thread that waits for the pending queue to drain, then re-subscribes. This is necessary because RabbitMQ pushes messages to the client — you can't just "pause."
- **SQLConsumer**: Stops polling the `sz_message_queue` table. Uses progressive backoff on empty queue (sleep 1s, doubling up to `MAXIMUM_SLEEP_TIME` of 10s).

**Task-Level Throttling (SchedulingService)**

The scheduling service also provides implicit throttling:
- Tasks that can't acquire resource locks are postponed (moved to a postponed queue)
- Postponed tasks are retried at `POSTPONED_TIMEOUT` intervals (default 50ms)
- Only one postponed task can be in progress at a time
- This prevents overwhelming the database with conflicting transactions

**Connection Pool Exhaustion**

If all database connections are in use, `ConnectionProvider.getConnection()` blocks until one becomes available. This naturally throttles the number of concurrent database operations to `maxPoolSize` (= `coreConcurrency * 3`).

### Why this matters for code changes

When modifying consumer code, always preserve the backpressure contract:
- Never bypass `throttleConsumption()` / resume checks
- Never allow unbounded queuing of messages
- The RabbitMQ cancel/resubscribe pattern is intentional — don't simplify it to a pause flag
