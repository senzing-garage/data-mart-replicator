## Idle Detection (waitUntilIdle)

### Why is idle detection needed?

Because everything in the replicator is asynchronous — INFO messages are queued, processed into scheduled tasks, handled by task handlers that may trigger follow-up tasks, which generate pending report updates that spawn more tasks — there is no simple "done" signal. After loading data into the Senzing repository, you need a way to know when the data mart has finished computing all statistics.

`SzReplicator.waitUntilIdle()` solves this by checking that the replicator has been idle across ALL layers for a specified duration.

### How it works — two-tier check

**Tier 1: SzReplicator.waitUntilIdle(idleTime, maxWaitTime)**

Polls in a loop (sleeping 1 second between checks):

1. Check `messageConsumer.getMessageCount()` — are there any pending messages?
2. If zero messages, check `messageConsumer.getLastMessageNanoTime()` — has enough time elapsed since the last message was seen?
3. If the message layer has been idle for `idleTime` milliseconds, delegate to tier 2: `replicatorService.waitUntilIdle(idleTime, remainingTime)`
4. If tier 2 also reports idle → return `true`
5. Otherwise, loop until `maxWaitTime` expires

**Tier 2: SzReplicatorService.waitUntilIdle(idleTime, maxWaitTime)**

Also polls in a loop:

1. Check `getPendingReportUpdateCount()` — are there pending report updates in `sz_dm_pending_report`?
2. Check `schedulingService.getAllRemainingTasksCount()` — are there scheduled/postponed tasks?
3. If both are zero, check BOTH timestamps:
   - `lastReportActivityNanoTime` — when was the last report update activity?
   - `schedulingService.getLastTaskActivityNanoTime()` — when was the last task activity?
4. If BOTH have been idle for at least `idleTime` milliseconds → return `true`
5. Otherwise, loop until `maxWaitTime` expires

### The three idle conditions (all must be true simultaneously)

For the replicator to be considered idle, ALL of these must hold for at least `idleTime` milliseconds:

1. **No pending messages** — `messageConsumer.getMessageCount() == 0` and no message seen for `idleTime`
2. **No scheduled tasks** — `schedulingService.getAllRemainingTasksCount() == 0` and no task activity for `idleTime`
3. **No pending report updates** — `getPendingReportUpdateCount() == 0` and no report activity for `idleTime`

### Parameters

- `idleTime` — How long (ms) ALL layers must be idle before returning true. Longer values are more conservative but slower to return.
- `maxWaitTime` — Maximum time (ms) to wait. Zero = check once without waiting. Negative = wait indefinitely.
- Returns `true` if idle, `false` if maxWaitTime expired without reaching idle.

### Why time-based rather than event-based?

A message could be in transit (not yet arrived at SQS/RabbitMQ/SQL queue) when we check. If we only checked "is anything pending right now?", we'd get false positives. The time-based approach says "nothing has happened for N milliseconds" which provides high confidence that no more work is coming — assuming the caller knows they've stopped sending records to Senzing.

### Usage in tests

Unit tests use `waitUntilIdle()` after loading test data to ensure all entity refreshes, follow-up tasks, and report updates have completed before asserting on report values. Without this, tests would be flaky due to the asynchronous processing pipeline.

### Diagnostic logging

When idle detection fails (returns false), both tiers log diagnostic info:
- Remaining message count and message idle time
- Remaining scheduled tasks and task idle time
- Remaining report updates and report idle time

This helps diagnose what's still in-flight when the system won't settle.
