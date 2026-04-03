## Report Model Hierarchy and REST API

### How are data mart reports structured and served?

The report system has three layers: **model classes** (the data), **query classes** (SQL against sz_dm_report), and **service interfaces** (REST endpoints via Armeria).

### Report Model Hierarchy (com.senzing.datamart.reports.model)

**Root Report: SzSummaryStats**
```
SzSummaryStats
└── sourceSummaries: SortedMap<String, SzSourceSummary>  (one per data source)
    └── SzSourceSummary
        ├── dataSource: String
        ├── entityCount: long
        ├── recordCount: long
        ├── unmatchedRecordCount: long
        └── crossSummaries: SortedMap<String, SzCrossSourceSummary>  (one per "versus" data source)
            └── SzCrossSourceSummary
                ├── dataSource: String (primary/"from")
                ├── versusDataSource: String (the "to/versus" — can be SAME as primary)
                ├── matches: SortedMap<SzCountsKey, SzMatchCounts>
                ├── ambiguousMatches: SortedMap<SzCountsKey, SzRelationCounts>
                ├── possibleMatches: SortedMap<SzCountsKey, SzRelationCounts>
                ├── possibleRelations: SortedMap<SzCountsKey, SzRelationCounts>
                └── disclosedRelations: SortedMap<SzCountsKey, SzRelationCounts>
```

**Key detail**: The "versus" data source CAN be the same as the primary data source. So a `SzSourceSummary` for "CUSTOMERS" will have a cross-summary to "CUSTOMERS" (same-source dedup statistics) AND cross-summaries to every other data source.

**SzCountsKey** — Composite key for match key + principle breakdowns:
- `matchKey: String` (nullable) — which attributes matched
- `principle: String` (nullable) — which ER rule fired
- Comparable: null sorts before non-null

**SzMatchCounts** — For record-level matches (entities resolved together):
- `entityCount: long` — entities where records from primary source matched versus source
- `recordCount: long` — records from the PRIMARY source only (not total)

**SzRelationCounts** — For relationship-level statistics:
- `entityCount: long` — entities from primary source with this relation type to versus source
- `recordCount: long` — records from primary source in those entities
- `relationCount: long` — count of relationships

**SzLoadedStats** — A report on data loading status, derived from DSS:
```
SzLoadedStats
├── totalRecordCount: long       — all records loaded across all data sources
├── totalEntityCount: long       — all entities across all data sources
├── totalUnmatchedRecordCount: long — total singleton (unmatched) records
└── dataSourceCounts: SortedMap<String, SzSourceLoadedStats>
    └── SzSourceLoadedStats
        ├── dataSource: String
        ├── recordCount: long    — records loaded from this data source
        ├── entityCount: long    — entities representing this data source
        └── unmatchedRecordCount: long — records from this source that are singletons
```

This answers "what's been loaded and how well did it resolve?" at a glance:
- High `unmatchedRecordCount` relative to `recordCount` suggests poor matching for that source
- `entityCount` much lower than `recordCount` indicates good deduplication
- Built by `LoadedStatsReports` which queries DSS rows for ENTITY_COUNT, RECORD_COUNT, and UNMATCHED_COUNT per data source, then aggregates totals

**SzEntitySizeBreakdown** — Map of entity size → SzEntitySizeCount (entitySize, entityCount)

**SzEntityRelationsBreakdown** — Map of relations count → SzEntityRelationsCount (relationsCount, entityCount)

**Pagination models**: SzEntitiesPage, SzRelationsPage, SzReportEntity, SzReportRecord, SzReportRelation for drill-down queries against sz_dm_report_detail.

### Query Classes (com.senzing.datamart.reports)

Static methods that execute SQL queries against sz_dm_report and sz_dm_report_detail:

- **SummaryStatsReports** — Builds SzSummaryStats by querying DSS and CSS rows, assembling the cross-summary hierarchy. Supports optional matchKey/principle filtering.
- **LoadedStatsReports** — Derives SzLoadedStats from DSS entity/record counts.
- **EntitySizeReports** — Queries ESB rows to build SzEntitySizeBreakdown.
- **EntityRelationsReports** — Queries ERB rows to build SzEntityRelationsBreakdown.
- **ReportUtilities** — Shared pagination helpers (retrieveEntitiesPage, retrieveRelationsPage) with configurable page size and sampling.

### Service Interfaces and REST Endpoints

Service interfaces define REST endpoints with default methods that delegate to the query classes. All implemented by **DataMartReportsServices** which holds the SzEnvironment and ConnectionProvider.

**Key REST paths:**
```
/statistics/summary/                                          → SzSummaryStats
/statistics/summary/data-sources/{ds}                         → SzSourceSummary
/statistics/summary/data-sources/{ds}/vs/{vsDs}               → SzCrossSourceSummary
/statistics/summary/data-sources/{ds}/vs/{vsDs}/matches       → match entities/records
/statistics/summary/data-sources/{ds}/vs/{vsDs}/ambiguous-matches
/statistics/summary/data-sources/{ds}/vs/{vsDs}/possible-matches
/statistics/summary/data-sources/{ds}/vs/{vsDs}/possible-relations
/statistics/summary/data-sources/{ds}/vs/{vsDs}/disclosed-relations
```

### How DSS and CSS map to the model

- **DSS rows** with `data_source1 == data_source2` → same-source statistics within SzSourceSummary (entity count, matched/unmatched counts for same-source dedup)
- **DSS rows** with `data_source1 == data_source2` + match key/principle → same-source SzCrossSourceSummary to itself
- **CSS rows** with `data_source1 != data_source2` → SzCrossSourceSummary for cross-source pairs
- The SzCrossSourceSummary model is the SAME structure regardless of whether the versus data source is the same or different — the distinction is only in which report code (DSS vs CSS) the data comes from

### Why same-source pairs use DSS instead of CSS

Same-source dedup statistics (e.g., how many CUSTOMERS records matched other CUSTOMERS records) are stored under `DATA_SOURCE_SUMMARY` because they're fundamentally about that data source's quality. Cross-source overlap (CUSTOMERS vs VENDORS) is stored under `CROSS_SOURCE_SUMMARY`. But at the model level, both are represented as SzCrossSourceSummary — the REST API presents a unified view.
