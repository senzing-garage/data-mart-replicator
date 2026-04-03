## Incremental Report Updates

### How does the pending report update queue work?

Reports are updated incrementally through a three-stage pipeline:

**Stage 1: Delta Generation (RefreshEntityHandler)**

When an entity is refreshed, `EntityDelta` computes what changed and inserts rows into `sz_dm_pending_report`:

```sql
INSERT INTO sz_dm_pending_report
  (report_key, entity_delta, record_delta, relation_delta, entity_id, related_id)
VALUES (?, ?, ?, ?, ?, ?)
```

Each row represents a +/- adjustment to a report statistic. For example, if an entity grew from 2 to 3 records:
- Entity size 2: `entity_delta = -1` (one fewer entity of size 2)
- Entity size 3: `entity_delta = +1` (one more entity of size 3)

**Stage 2: Periodic Scheduling (ReportUpdater)**

A background thread (`ReportUpdater`) runs on a configurable period. It:
1. Collects distinct report keys from pending updates
2. Schedules an appropriate report update task for each key (`UPDATE_DATA_SOURCE_SUMMARY`, `UPDATE_CROSS_SOURCE_SUMMARY`, etc.)
3. Commits the scheduled tasks

This batching means many entity changes accumulate before a single report update runs.

**Stage 3: Lease-Based Processing (UpdateReportHandler)**

Report handlers use a lease pattern for safe concurrent processing:

1. **Lease**: Update pending rows with a unique `lease_id` and `expire_lease_at`:
   ```sql
   UPDATE sz_dm_pending_report
   SET lease_id = ?, expire_lease_at = ?
   WHERE report_key = ? AND (lease_id IS NULL OR expire_lease_at < NOW())
   ```

2. **Accumulate**: Sum all leased deltas:
   ```sql
   SELECT SUM(entity_delta), SUM(record_delta), SUM(relation_delta)
   FROM sz_dm_pending_report
   WHERE report_key = ? AND lease_id = ?
   ```

3. **Apply**: Update the report table:
   ```sql
   UPDATE sz_dm_report
   SET entity_count = entity_count + ?, record_count = record_count + ?
   WHERE report_key = ?
   ```

4. **Cleanup**: Delete processed rows:
   ```sql
   DELETE FROM sz_dm_pending_report WHERE lease_id = ?
   ```

All four steps happen within a single transaction.

### Why leasing instead of simple SELECT FOR UPDATE?

Leasing handles crash recovery. If a replicator instance crashes mid-update:
- The leased rows aren't deleted
- The `expire_lease_at` timestamp eventually passes
- Another instance (or the restarted instance) can re-lease and reprocess them
- No deltas are lost

### Critical correctness rule

**Delta signs must be balanced.** Every increment must have a corresponding decrement when state changes. For example, when an entity moves from size 2 to size 3:
- You MUST insert `entity_delta = -1` for size 2
- You MUST insert `entity_delta = +1` for size 3
- Missing either one permanently skews the report counts

The same applies to cross-source reports when match keys or ER codes change — the old combination must be decremented.

### Report detail updates

In addition to the summary `sz_dm_report` table, handlers also update `sz_dm_report_detail` which tracks per-entity contributions to each report. This enables drill-down queries like "which entities contribute to the CUSTOMERS↔VENDORS matched count?"
