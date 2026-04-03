## Thread Safety and Locking

### How does the replicator prevent data corruption with concurrent entity updates?

The system uses a multi-layered locking strategy:

**1. ResourceKey — The Universal Resource Identifier for Locking**

`ResourceKey` is a `Comparable` class from the generalized Senzing Listener `LockingService` that uniquely identifies a lockable resource. It provides deterministic sort ordering, which is the foundation of the deadlock avoidance strategy. ResourceKey is reused throughout the data mart (not just in the LockingService) to describe resources that require locking.

Resource key format:
- `ENTITY:{entityId}` — for entity modifications
- `RECORD:{dataSource}:{recordId}` — for record modifications
- `RELATIONSHIP:{lowerEntityId}:{higherEntityId}` — for relationship modifications (IDs always ordered lower < higher)

**2. The Locking Pattern: Collect, Sort, Lock All Up-Front**

The core pattern used by `RefreshEntityHandler`:

1. Compute the `EntityDelta` to determine which resources need updating
2. Get all required `ResourceKey` instances from the delta via `entityDelta.getResourceKeys()` — this returns a `SortedSet<ResourceKey>` (already sorted)
3. **Before any other work**, enroll ALL resource keys into `sz_dm_locks` in sorted order via batch update:

```java
String operationId = this.generateOperationId(entityDelta.getEntityId());

ps = conn.prepareStatement(
    "INSERT INTO sz_dm_locks AS t1 (resource_key, modifier_id) "
        + "VALUES (?, ?) "
        + "ON CONFLICT (resource_key) DO UPDATE SET"
        + " modifier_id = EXCLUDED.modifier_id");

SortedSet<ResourceKey> resourceKeys = entityDelta.getResourceKeys();

this.batchUpdate(ps, resourceKeys, (ps2, resourceKey) -> {
    ps2.setString(1, resourceKey.toString());
    ps2.setString(2, operationId);
    return 1;
});
```

On PostgreSQL, each `ON CONFLICT DO UPDATE` acquires a row-level lock that **blocks** if another process or thread has already updated that same row within an uncommitted transaction. By locking all resources up-front before doing any other work, we ensure mutual exclusion without deadlocks (because of the consistent sort order).

**3. Process-Level LockingService (Optimization Layer)**

The `LockingService` interface is a generalized Senzing Listener abstraction that could have a multi-process implementation, but currently only has a **process-scoped** implementation (`ProcessScopeLockingService`). The `SchedulingService` uses it to check resource availability before dispatching a task. If another task within the same JVM already holds locks on the same resources, the new task is **postponed** (not failed) and retried later. This is an optimization to prevent wasted work — it avoids starting a transaction that would just block at the database level. The actual correctness guarantee comes from the database-level `sz_dm_locks` enrollment.

**4. Transaction Scope**

All entity/record/relationship modifications happen within a single database transaction:
1. Begin transaction (auto-commit OFF)
2. Enroll ALL resource locks in sorted order (blocks if contended)
3. Update entity row
4. Update/insert record rows
5. Orphan removed records
6. Update/insert/delete relationship rows
7. Insert pending report updates
8. Commit (releases all row-level locks atomically)

If any step fails, the entire transaction rolls back — no partial updates are ever visible.

### Why locking must happen FIRST

The lock enrollment step must come before any other modifications in the transaction. If you update entity/record/relationship rows first and THEN try to enroll locks, another concurrent transaction could have already modified those same rows — leading to inconsistent state. Lock first, then work.

### Critical invariant

**Never modify entity, record, or relationship rows outside of the RefreshEntityHandler's transaction pattern.** The lock enrollment step is essential — without it, concurrent updates to the same entity from different threads (or different replicator instances) would produce corrupt data.
