## Report Handler Hierarchy

### How do report update handlers work?

**UpdateReportHandler** is the base class for all report update tasks. It implements a lease-based pipeline:

**Lifecycle of a report update task (UpdateReportHandler.handleTask):**

1. **Lease pending rows** — `leaseReportUpdates()` atomically claims rows from `sz_dm_pending_report` by setting `lease_id` and `expire_lease_at`. Commits the lease immediately so other handlers can proceed on different report keys.

2. **Update report statistic** — `updateReportStatistic()` sums all entity/record/relation deltas from the leased rows, then upserts to `sz_dm_report` with `INSERT ... ON CONFLICT DO UPDATE`, incrementing the counts. Before applying, it calls the override hooks (`overrideEntityDelta()`, `overrideRecordDelta()`, `overrideRelationDelta()`) to let subclasses adjust.

3. **Update report details** — `updateReportDetails()` builds a delta map keyed by entity ID (or entity:relatedId pair for relationships). Upserts to `sz_dm_report_detail` with `stat_count = stat_count + delta`. Deletes any rows where `stat_count` drops to zero (entity no longer contributes to this statistic).

4. **Delete leased rows** — `deleteLeasedReportUpdates()` removes the processed rows from `sz_dm_pending_report`. Verifies the delete count matches expectations.

5. **Lease duration check** — if the entire operation took more than 60 seconds (`LEASE_DURATION`), the transaction is rolled back to prevent double-counting. Old leases (exceeding 2x lease duration) are automatically expired for crash recovery.

6. **Commit** — all changes are committed atomically.

### The concrete handler classes

| Handler | Task Action | Custom Logic |
|---------|-------------|-------------|
| `SourceSummaryReportHandler` | `UPDATE_DATA_SOURCE_SUMMARY` | Overrides `overrideRecordDelta()` for orphaned record maintenance |
| `CrossSummaryReportHandler` | `UPDATE_CROSS_SOURCE_SUMMARY` | None — pure inheritance from base |
| `SizeBreakdownReportHandler` | `UPDATE_ENTITY_SIZE_BREAKDOWN` | None — pure inheritance from base |
| `RelationBreakdownReportHandler` | `UPDATE_ENTITY_RELATION_BREAKDOWN` | None — pure inheritance from base |

Most concrete handlers simply extend `UpdateReportHandler` and pass the appropriate `TaskAction` to the constructor. The base class handles all the leasing, updating, and cleanup logic.

### SourceSummaryReportHandler — Orphaned Record Maintenance

`SourceSummaryReportHandler` overrides `overrideRecordDelta()` to handle records that have been orphaned in the data mart (entity_id = 0). This is critical periodic maintenance that only triggers for the `ENTITY_COUNT` statistic.

**What are orphaned records?**
When `RefreshEntityHandler` discovers that a record is no longer part of an entity, it sets `entity_id = 0` rather than deleting the record. This is because the record may have moved to another entity — but that entity's refresh hasn't been processed yet.

**The orphan recovery process:**

1. **Lease orphaned record rows** — Updates `sz_dm_record` rows with `entity_id = 0` for the relevant data source, setting `modifier_id` to claim them.

2. **For each orphaned record, consult the Senzing SDK:**
   - Calls `engine.getEntity(recordKey, flags)` to find the record's current entity
   - If the SDK says the record no longer exists → mark for deletion
   - If the SDK says the record belongs to an entity → check if that entity is already in the data mart

3. **Handle the race condition:**
   - If the entity IS in the data mart → **reconnect** the record (update `entity_id`, `match_key`, `errule_code` to current values)
   - If the entity is NOT yet in the data mart → schedule a `REFRESH_ENTITY` follow-up task for that entity (another process is still working on it)

4. **Cleanup:**
   - Delete truly orphaned records (the record was deleted from Senzing entirely)
   - Return `computedDelta + reconnectedCount` — the reconnected records adjust the record count

**Why this matters:**
Without this maintenance, records that moved between entities during concurrent processing could be permanently orphaned. The periodic reconciliation ensures the data mart eventually becomes consistent, even when processing order causes temporary inconsistencies.

### ReportUpdater Background Thread

The `ReportUpdater` is an inner class of `SzReplicatorService` that runs as a background thread:

- **Accumulates report keys** — `RefreshEntityHandler` calls `scheduleReportFollowUp()` which adds report keys to the updater's map
- **Periodically schedules tasks** — on a configurable period, iterates all accumulated report keys and schedules the appropriate update task for each
- **Deduplicates naturally** — since the map is keyed by `SzReportKey`, multiple requests for the same report just overwrite the same entry
- **Handles startup recovery** — on initialization, loads any leftover pending report updates from the database (from a prior run that didn't complete)
- **Graceful shutdown** — stops scheduling, joins the thread
