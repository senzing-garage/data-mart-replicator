## Multi-Process Safety

### How do multiple replicator instances safely share the same data mart database?

Multiple replicator instances can run concurrently against the same PostgreSQL database. Safety is ensured through:

**1. Database-Level Locking via sz_dm_locks**

The `INSERT ... ON CONFLICT DO UPDATE` pattern on the `sz_dm_locks` table creates row-level locks that span transaction boundaries. When Instance A is modifying Entity 42, Instance B's attempt to lock the same entity blocks until A commits or rolls back. This is a PostgreSQL-level guarantee — no application-level coordination needed.

**2. Report Update Leasing**

The `sz_dm_pending_report` table uses a lease-based pattern for multi-instance safety:
- A report handler "leases" pending updates by setting `lease_id` and `expire_lease_at`
- Other instances skip leased rows
- If a lease expires (instance crashed), another instance can pick up the work
- After applying deltas, the handler deletes only its own leased rows

This prevents double-counting: each pending report update is processed by exactly one instance.

**3. SQL Message Queue Leasing (SQLConsumer)**

When using the SQL-based message queue (`sz_message_queue`), messages are leased similarly:
- Consumer marks messages with a lease ID and expiration
- Other consumers skip leased messages
- Expired leases are available for reprocessing

**4. Entity Hash Idempotency**

Even if two instances process the same entity concurrently, the entity hash comparison ensures correctness. If the entity hasn't changed since the last stored hash, no modifications are made. This makes reprocessing safe.

### What doesn't work with multiple instances

- **SQLite**: SQLite does not support concurrent writers. Multi-instance deployment requires PostgreSQL.
- **Process-level LockingService**: The in-memory `ProcessScopeLockingService` only coordinates within a single JVM. Cross-instance coordination relies entirely on database-level locks.

### Relationship ID normalization

Relationships always store `entity_id < related_id`. This normalization prevents two instances from creating duplicate relationship rows with swapped IDs. The primary key `(entity_id, related_id)` enforces uniqueness.
