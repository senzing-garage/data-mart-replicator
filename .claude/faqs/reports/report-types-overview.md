## Report Types Overview

### What reports does the data mart maintain?

The data mart maintains four types of summary reports, all updated incrementally via delta accumulation rather than full recomputation.

**1. Data Source Summary (DSS)** — The "header" for each data source
- One report per data source (data_source1 == data_source2 in the report key)
- Acts as a high-level summary of a data source's presence in the entity resolution
- `ENTITY_COUNT`: All entities having at least one record from this data source
- `RECORD_COUNT`: Total records from this data source
- `UNMATCHED_COUNT`: Singleton entities — entities with only 1 record from this data source and no matches
- Example: "CUSTOMERS has 10,000 entities containing 12,500 records, of which 3,000 are unmatched singletons"
- Handler: `SourceSummaryReportHandler` (also handles orphaned record maintenance)

**2. Cross-Source Summary (CSS)** — Matching and relationship statistics between data source pairs
- One report per ordered pair of data sources, **including same-source pairs**
- Tracks both matches (records resolving together) and relationships (entity-to-entity links)
- Statistics: `MATCHED_COUNT`, `AMBIGUOUS_MATCH_COUNT`, `POSSIBLE_MATCH_COUNT`, `DISCLOSED_RELATION_COUNT`, `POSSIBLE_RELATION_COUNT`
- All further broken down by **ER Code (principle)**, **Match Key**, and **both combined**
- Handler: `CrossSummaryReportHandler`

**Same-source CSS (source1 == source2) — Deduplication metrics:**
- `MATCHED_COUNT` where both sources are CUSTOMERS = entities with **2 or more** records from CUSTOMERS that resolved together
- This measures within-source duplication: how many entities represent deduplicated CUSTOMERS records
- Relationship statistics track relations between entities that both have CUSTOMERS records

**Cross-source CSS (source1 != source2) — Cross-referencing metrics:**
- `MATCHED_COUNT` where source1=CUSTOMERS, source2=VENDORS = entities containing records from **both** CUSTOMERS and VENDORS
- This measures cross-source overlap: how the same real-world entity appears in different data sources
- Relationship statistics track relations between CUSTOMERS-containing entities and VENDORS-containing entities

**3. Entity Size Breakdown (ESB)**
- Distribution of entities by how many records they contain
- Tracks entity count at each size
- Example: "8,000 entities have 1 record; 1,500 have 2 records; 300 have 3-5 records"
- Handler: `SizeBreakdownReportHandler`

**4. Entity Relation Breakdown (ERB)**
- Distribution of entities by how many relationships they have
- Tracks entity count at each relationship count
- Example: "7,000 entities have 0 relations; 2,000 have 1-2 relations; 500 have 3+ relations"
- Handler: `RelationBreakdownReportHandler`

### Report Key Structure

Reports are identified by `SzReportKey` with format:
```
{REPORT_CODE}:{STATISTIC}[:{DATA_SOURCE1}[:{DATA_SOURCE2}]]
```

Examples:
- `DSS:ENTITY_COUNT:CUSTOMERS:CUSTOMERS` — all entities with at least 1 CUSTOMERS record
- `DSS:UNMATCHED_COUNT:CUSTOMERS:CUSTOMERS` — singleton CUSTOMERS entities
- `CSS:MATCHED_COUNT:CUSTOMERS:CUSTOMERS` — entities with 2+ CUSTOMERS records (dedup count)
- `CSS:MATCHED_COUNT:CUSTOMERS:VENDORS` — entities with records from both sources
- `CSS:POSSIBLE_MATCH_COUNT:CUSTOMERS:VENDORS` — possible match relationships across sources
- `CSS:MATCHED_COUNT:CUSTOMERS:VENDORS:MATCH_NAME_DOB:+NAME+DOB` — cross-source matches filtered by principle and match key
- `ESB:3` — entities with exactly 3 records
- `ERB:2` — entities with exactly 2 relationships

### The distinction between DSS and CSS

Think of DSS as the **header** for a data source — "how many entities and records does this source have?" The CSS is the **detail** — "how do records from this source match or relate to records from another source (or itself)?" Same-source CSS pairs reveal deduplication quality; cross-source CSS pairs reveal cross-reference value.

### How reports are updated

Reports are NOT recomputed from scratch. Instead:
1. `RefreshEntityHandler` computes deltas (old count vs new count) via `EntityDelta`
2. Deltas are batch-inserted into `sz_dm_pending_report` as +/- values
3. `ReportUpdater` background thread periodically schedules report update tasks
4. Report handlers lease pending deltas, apply them to `sz_dm_report` and `sz_dm_report_detail`, then delete processed rows

This means report counts are running totals adjusted by deltas. Getting the math wrong (e.g., missing a delta or double-counting) permanently skews the reports.
