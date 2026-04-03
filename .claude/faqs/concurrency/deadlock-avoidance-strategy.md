## Deadlock Avoidance Strategy

### How does the replicator avoid deadlocks when it needs to update related entities?

This is one of the most critical design decisions in the entire system.

**The Problem:**
When refreshing Entity A, the handler may discover that Entity A now has a relationship with Entity B (or lost one). Both entities need updating — Entity A's relationship data AND Entity B's relationship data must be consistent. But if Thread 1 holds locks on Entity A and tries to lock Entity B, while Thread 2 holds locks on Entity B and tries to lock Entity A — deadlock.

**The Solution: Lease and Schedule Follow-Up**

The replicator NEVER tries to lock additional resources within an existing transaction. Instead:

1. **Lease resources**: The handler acquires locks on all resources it knows about at the START of the transaction (entity, its records, its known relationships) — all enrolled in sorted order.

2. **Do the work**: Update entity, records, and relationships within the transaction.

3. **Discover additional work**: If the handler determines that a related entity (e.g., Entity B) also needs updating (relationship added/removed/changed), it does NOT try to lock Entity B.

4. **Schedule a follow-up task**: Instead, it schedules a `REFRESH_ENTITY` follow-up task for Entity B. This follow-up will execute AFTER the current transaction commits and releases all its locks.

5. **Follow-up executes independently**: When the follow-up task runs, it acquires its OWN locks on Entity B (and B's resources), does its work, and commits independently.

**Why this works:**
- No transaction ever holds locks on more than one entity's resource set at a time
- Follow-up tasks run after locks are released, so there's no lock contention between related entity updates
- The follow-up task will fetch the CURRENT state of Entity B from the SDK, so it always sees the latest data
- Follow-up deduplication collapses multiple requests to refresh the same entity

**The same principle applies to report updates:**
Instead of the entity refresh handler directly updating report tables (which would cause massive contention since many entity updates affect the same reports), it inserts delta rows into `sz_dm_pending_report`. These are processed later in periodic batches by the report handlers, which have their own separate lease-based processing. This avoids every entity refresh task from contending on the same report rows.

### Critical invariant

**NEVER acquire additional resource locks within an existing transaction.** If you need to modify another entity, schedule a follow-up task. If you need to update reports, insert into the pending report queue. This is the fundamental principle that prevents deadlocks in concurrent and multi-process deployments.
