## Database Schema

### What tables does the data mart use?

The schema is auto-created on first run by `PostgreSQLSchemaBuilder` or `SQLiteSchemaBuilder`.

**Core Data Tables:**

| Table | Purpose | Primary Key |
|-------|---------|-------------|
| `sz_dm_entity` | Resolved entity state | `entity_id` |
| `sz_dm_record` | Individual source records within entities | `(data_source, record_id)` |
| `sz_dm_relation` | Relationships between entities | `(entity_id, related_id)` |

**Report Tables:**

| Table | Purpose | Primary Key |
|-------|---------|-------------|
| `sz_dm_report` | Aggregated report statistics | `report_key` |
| `sz_dm_report_detail` | Per-entity report detail rows | `(report_key, entity_id, related_id)` |
| `sz_dm_pending_report` | Work queue of pending report deltas | (no PK — work queue) |

**Infrastructure Tables:**

| Table | Purpose | Primary Key |
|-------|---------|-------------|
| `sz_dm_locks` | Distributed resource locking | `resource_key` |
| `sz_message_queue` | SQL-based message queue (optional) | (message-specific) |

### Key columns and their meanings

**sz_dm_entity:**
- `entity_hash`: Compressed base64 JSON of the full entity state — used for change detection
- `prev_entity_hash`: Previous hash before the last update — enables rollback analysis
- `record_count` / `relation_count`: Denormalized counts for size/relation breakdowns
- `creator_id` / `modifier_id`: Operation IDs for auditing (unique per task execution)

**sz_dm_record:**
- `entity_id`: The entity this record belongs to. **0 = orphaned** (record removed from entity but not yet reclaimed or deleted)
- `match_key`: The attributes that matched this record into the entity (null for seed record)
- `errule_code`: The ER rule (principle) that matched this record (null for seed record)
- `prev_entity_id`: Previous entity before a re-match — tracks record migration between entities
- `adopter_id`: Operation ID that reclaimed this orphaned record

**sz_dm_relation:**
- `entity_id` is always < `related_id` (normalized ordering)
- `match_key`: Forward match key (lower entity → higher entity)
- `rev_match_key`: Reverse match key (higher entity → lower entity) — differs for directional keys like REL_POINTER
- `match_type`: AMBIGUOUS_MATCH, POSSIBLE_MATCH, DISCLOSED_RELATION, or POSSIBLE_RELATION
- `relation_hash` / `prev_relation_hash`: Compressed state for change detection

### Orphaned records (entity_id = 0)

When an entity loses a record, the record is NOT deleted. Instead, `entity_id` is set to 0 (orphaned). This serves two purposes:
1. Another entity may adopt the orphaned record (the record still exists in Senzing, just resolved differently)
2. The `SourceSummaryReportHandler` has special logic to reconnect orphaned records to their new entities or clean them up

### PostgreSQL vs SQLite differences

- PostgreSQL uses `BIGINT` for entity IDs; SQLite uses `INTEGER`
- PostgreSQL has triggers for automatic timestamp maintenance; SQLite uses separate INSERT/UPDATE triggers
- PostgreSQL supports `INSERT ... ON CONFLICT DO UPDATE` for upserts; SQLite uses similar syntax
- PostgreSQL supports `SELECT FOR UPDATE` row locking; SQLite relies on its single-writer model
- Only PostgreSQL supports multi-instance deployment
