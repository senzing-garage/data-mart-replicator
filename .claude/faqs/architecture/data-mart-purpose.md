## Data Mart Purpose

### What is the Senzing Data Mart and why does it exist?

The Senzing entity resolution engine stores its working data in an internal database optimized for resolution processing — not for analytics or reporting. You cannot (and should not) query Senzing's internal tables directly for reporting purposes.

The **Data Mart Replicator** solves this by maintaining a separate analytical database (the "data mart") that mirrors the resolved entity state in a queryable, relational form. It provides:

**1. Entity/Record/Relationship Tables**
- `sz_dm_entity`: One row per resolved entity with name, record count, relation count
- `sz_dm_record`: One row per source record, showing which entity it belongs to and how it matched
- `sz_dm_relation`: One row per entity-to-entity relationship with match type, match key, and ER rule

**2. Pre-Aggregated Reports (four types)**
- **Data Source Summary (DSS)**: Per-datasource entity and record counts
- **Cross-Source Summary (CSS)**: How records from different data sources match/relate, broken down by ER code and match key
- **Entity Size Breakdown (ESB)**: Distribution of entities by record count (singletons, pairs, etc.)
- **Entity Relation Breakdown (ERB)**: Distribution of entities by relationship count

**3. Real-Time Incremental Updates**
Rather than periodically re-exporting all entities, the replicator consumes Senzing INFO messages (emitted on every record add/modify/delete) and incrementally updates only what changed. This enables near-real-time reporting even at billion-entity scale.

### What questions can the data mart answer?

- How many entities span both CUSTOMERS and VENDORS data sources?
- What match keys are driving cross-source resolution? (e.g., +NAME+ADDRESS vs +SSN)
- How many entities have 50+ records? (potential over-matching)
- What are the possible matches (entity pairs that almost resolved)?
- Which ER rules fire most frequently?
- What's the record-to-entity compression ratio per data source?

### Why not just use the SDK export?

Full entity export works for initial population or one-time analysis, but:
- It requires reading every entity — O(N) regardless of how much changed
- It can't provide real-time reporting during continuous data loading
- It's impractical at scale (billions of entities)

The incremental replication approach processes only affected entities — typically a tiny fraction of the total — making it suitable for production continuous operation.
