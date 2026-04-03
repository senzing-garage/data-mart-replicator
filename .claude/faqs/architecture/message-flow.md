## End-to-End Message Flow

### How does a Senzing INFO message become database updates?

The full pipeline from INFO message to data mart state change:

1. **Message Arrival**: A message consumer (SQS, RabbitMQ, or SQL) receives an INFO message from Senzing. The message is JSON describing what changed — which record was added/modified/deleted and which entities were affected.

2. **Batching**: The consumer groups messages into `MessageBatch` objects. Each batch may contain multiple INFO messages (a single SQS/RabbitMQ message can contain a JSON array of INFO messages). The batch tracks completion of all its constituent messages.

3. **Worker Dispatch**: `AbstractMessageConsumer` dequeues messages and submits them to an `AsyncWorkerPool` for parallel processing. Each worker thread calls `SzReplicatorService.process()`.

4. **INFO Parsing**: `AbstractListenerService.scheduleTasks()` parses the INFO message via `SzInfoMessage.fromRawJson()`. The key fields are:
   - `DATA_SOURCE` + `RECORD_ID`: The source record that changed
   - `AFFECTED_ENTITIES`: Array of entity IDs impacted by the change

5. **Task Scheduling**: For each affected entity, a `REFRESH_ENTITY` task is scheduled via the `SchedulingService`. The task carries the `entityId` as a parameter and declares `ENTITY:{entityId}` as a lockable resource.

6. **Task Execution**: `RefreshEntityHandler` executes the task:
   - Retrieves current entity state from the Senzing SDK
   - Compares against the stored entity hash in `sz_dm_entity`
   - Computes an `EntityDelta` if changes detected
   - Within a single transaction: enrolls locks, updates entity/record/relationship rows, inserts pending report updates
   - Commits the transaction (releasing locks atomically)

7. **Follow-up Tasks**: If related entities changed, `REFRESH_ENTITY` follow-up tasks are scheduled for those entities. Report update deltas are written to `sz_dm_pending_report`.

8. **Report Updates**: A background `ReportUpdater` thread periodically schedules report update tasks (`UPDATE_DATA_SOURCE_SUMMARY`, `UPDATE_CROSS_SOURCE_SUMMARY`, `UPDATE_ENTITY_SIZE_BREAKDOWN`, `UPDATE_ENTITY_RELATION_BREAKDOWN`). These handlers lease pending deltas, apply them to `sz_dm_report` and `sz_dm_report_detail`, then delete the processed rows.

9. **Batch Completion**: Once all INFO messages in a batch are processed (success or failure), the batch is disposable and the original message is acknowledged/deleted from the queue.

### Why this matters

The pipeline is designed for **eventual consistency** with **at-least-once** semantics. Every step is idempotent — reprocessing the same INFO message produces the same result because entity hashes detect no-op changes. Report updates accumulate as deltas in a work queue, so they can be batched and applied efficiently rather than recomputing full aggregates on every change.
