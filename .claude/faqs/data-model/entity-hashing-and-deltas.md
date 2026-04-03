## Entity Hashing and Delta Computation

### How does the replicator detect what changed in an entity?

The replicator uses a **hash-then-diff** strategy to avoid unnecessary database writes.

**Step 1: Retrieve Current State from Senzing**
```java
String json = engine.getEntity(entityId, ENTITY_FLAGS);
SzResolvedEntity newEntity = SzResolvedEntity.parse(json);
```

The flags request comprehensive data: record matching info (match key + principle per record), relationship matching info (match key + principle per relation), data source summaries, etc.

**Step 2: Hash Comparison (Short-Circuit Path)**

The entity hash is a base-64 encoded ZIP'd version of a shortened JSON representation of the entity (with abbreviated property names to save space). In `RefreshEntityHandler` (lines 165-184):

- `ensureEntityRow()` or `prepareEntityDelete()` returns the stored `entity_hash` from `sz_dm_entity`
- A **null** hash means the entity doesn't exist in the data mart yet
- An **empty string** hash means the hashes match — no changes detected, short-circuit
- If the entity was not found in Senzing (deleted), a null `newEntity` triggers deletion logic

If the hash matches, the handler calls `ensureRelationIntegrity()` to verify relationships are consistent and returns — no `EntityDelta` is computed.

**Step 3: Edge Cases — Race Conditions with Created-Then-Deleted Entities**

An entity can be created and deleted before the data mart ever stores it. When an INFO message arrives for an entity that:
- Is not in the Senzing repository (deleted) AND
- Has no stored hash in the data mart (never replicated)

The handler recognizes this as "entity was never replicated" and follows up on any related entities that might need updating. This was a source of miscounted reports until it was fixed — the entity never contributed to report counts, so there's nothing to decrement.

**Step 4: Compute EntityDelta**

If hashes differ, decompress the old hash back to an `SzResolvedEntity` and compute detailed differences:

```java
SzResolvedEntity oldEntity = SzResolvedEntity.parseHash(entityHash);
EntityDelta delta = new EntityDelta(oldEntity, newEntity);
```

A secondary check (`Objects.equals(oldEntity, newEntity)`) catches the rare case where hashes differ but no semantic change exists.

### What EntityDelta Computes

The `EntityDelta` constructor does ALL difference computation in one pass:

**1. Data Source Deltas** (`dataSourceDeltas`)
- Per-data-source record count changes: e.g., CUSTOMERS went from 3→4 records (delta = +1)

**2. Record Changes**
- **Added records**: present in new entity, absent in old (key = `SzRecordKey`)
- **Removed records**: present in old, absent in new
- **Changed records**: present in both, but `matchKey` or `principle` (errule_code) differs — the record still belongs to the same entity, but HOW it matched changed

**3. Relationship Changes**
- **Added relations**: related entity present in new, absent in old
- **Removed relations**: related entity present in old, absent in new
- **Changed relations**: match type, match key, principle, or data sources of either side changed

**4. ResourceKey Collection**
All affected resources are collected into a `SortedSet<ResourceKey>` (for locking):
- `RECORD:{dataSource}:{recordId}` for added/removed/changed records
- `RELATIONSHIP:{lowerEntityId}:{higherEntityId}` for added/removed/changed relations

### Report Delta Functions (the 5 key functions)

**`findEntitySizeChanges()`** — Entity Size Breakdown (ESB)
- Compares old vs new record count. If different:
  - Decrement entity count at old size (if entity previously existed)
  - Increment entity count at new size (if entity still exists)
- Also tracks entity ID association/disassociation with the size statistic

**`findEntityRelationChanges()`** — Entity Relation Breakdown (ERB)
- Same pattern as size changes, but for relationship count
- Handles new entity (increment only), deleted entity (decrement only), changed count (decrement old + increment new)

**`findSourceSummaryChanges()`** — Data Source Summary (DSS), same-source pairs
- Uses `countMatches()` which crosses data source summaries with match key/principle pairs from records
- Match key/principle pairs come from `getMatchPairs()` which extracts all (matchKey, principle) combinations from the entity's constituent records
- Generates 4 variant combinations per record's match pair: (null, null), (matchKey, null), (null, principle), (matchKey, principle)
- Detects added/removed/changed same-source cross-match-key pairs
- Tracks `ENTITY_COUNT`, `MATCHED_COUNT`, and `UNMATCHED_COUNT` (singleton entities with only 1 record)

**`findCrossMatchChanges()`** — Cross-Source Summary (CSS), different-source pairs
- Same approach as `findSourceSummaryChanges()` but for cross-source pairs (source1 != source2)
- Tracks `MATCHED_COUNT` with match key and principle variants
- For each data source pair × match key/principle combination: entity delta (+1/-1) and record delta

**`findRelatedSourceChanges()`** — Cross-source relation statistics
- Tracks how many entities with records from Source A have **relationships** (not matches) to entities with records from Source B
- Uses `SourceRelationKey` which captures (matchType, matchKey, principle) from relationships
- Generates variant combinations per relation: total, by principle, by match key, and by both
- This is what enables filtering relation reports by principle, match key, or both

### How match key/principle variants enable report filtering

For **matched entities** (resolved together, not relations), the match keys and principles come from the entity's **constituent records** — specifically what attributes/rules caused each record to join the entity.

For **related entities** (relationships like POSSIBLE_MATCH, DISCLOSED_RELATION, etc.), the match keys and principles come from the **relationship itself** — what attributes/rules determined the relationship between the two entities.

In both cases, the `statisticVariants()` function generates all filter combinations:
1. Base statistic only (e.g., `POSSIBLE_MATCH_COUNT`)
2. With principle only (e.g., `POSSIBLE_MATCH_COUNT:CNAME_CFF_DEXCL`)
3. With match key only (e.g., `POSSIBLE_MATCH_COUNT:+NAME+ADDRESS-DOB`)
4. With both (e.g., `POSSIBLE_MATCH_COUNT:CNAME_CFF_DEXCL:+NAME+ADDRESS-DOB`)
5. Reverse match key variants (for asymmetric keys like `+REL_POINTER(SPOUSE:)`)

This means the `sz_dm_report` table has pre-aggregated counts at ALL granularity levels. Querying "how many POSSIBLE_MATCH relationships between CUSTOMERS and VENDORS were on the CNAME_CFF_DEXCL rule?" is just a single row lookup — no joins or aggregation needed.

### Two-Phase Report Updates

EntityDelta pre-computes all `SzReportUpdate` instances in its constructor. But additional report updates are added LATER by `RefreshEntityHandler` when it processes records and relationships:

- `createdRecord()` — called when a record was actually INSERT'd (not just adopted from orphan). Adds a `DATA_SOURCE_SUMMARY:ENTITY_COUNT` increment.
- `orphanedRecord()` — called when a removed record is orphaned (entity_id set to 0, no other entity claims it). Adds a `DATA_SOURCE_SUMMARY:ENTITY_COUNT` decrement.
- `trackStoredRelationship()` — called when a relationship is stored. Computes cross-source relationship count deltas between old and new relationship state, handling match type / principle / match key changes.
- `trackDeletedRelationship()` — called when a relationship is removed. Decrements all statistic variants.

All accumulated `SzReportUpdate` instances are then batch-inserted into `sz_dm_pending_report` for the `ReportUpdater` to process later.

### Critical correctness rules

1. **Delta signs must be balanced.** Every increment must have a corresponding decrement when state transitions. Missing either half permanently skews reports.
2. **All variants must be updated together.** If a relationship's principle changes, ALL variant combinations (base, by-principle, by-matchKey, by-both) must be decremented for the old values and incremented for the new values.
3. **EntityDelta computes deltas atomically** from full old-vs-new comparison. Do NOT try to compute deltas piecemeal or from partial state.
