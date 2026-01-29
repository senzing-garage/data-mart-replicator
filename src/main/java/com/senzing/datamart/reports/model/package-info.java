/**
 * This package provides model classes used in data mart reporting.
 *
 * &lt;h2&gt;Class Diagram&lt;/h2&gt;
 * &lt;pre&gt;
 * ═══════════════════════════════════════════════════════════════════════════════════
 *                     com.senzing.datamart.reports.model CLASS DIAGRAM
 * ═══════════════════════════════════════════════════════════════════════════════════
 *
 * ┌─────────────────────────────────────────────────────────────────────────────────┐
 * │                              ENUMERATIONS                                       │
 * ├─────────────────────────────────────────────────────────────────────────────────┤
 * │  «enum»                           «enum»                                        │
 * │  SzBoundType                      SzRelationType                                │
 * │  ├─ INCLUSIVE_LOWER               ├─ AMBIGUOUS_MATCH                            │
 * │  ├─ EXCLUSIVE_LOWER               ├─ POSSIBLE_MATCH                             │
 * │  ├─ INCLUSIVE_UPPER               ├─ POSSIBLE_RELATION                          │
 * │  └─ EXCLUSIVE_UPPER               └─ DISCLOSED_RELATION                         │
 * └─────────────────────────────────────────────────────────────────────────────────┘
 *
 * ┌──────────────────────────────────────────────────────────────────────────────────────────┐
 * │                          LOADED STATS HIERARCHY                                          │
 * ├──────────────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                          │
 * │  SzLoadedStats ─────────────────────────────1:* ────► SzSourceLoadedStats                │
 * │  ├─ getTotalRecordCount(): long                      ├─ getDataSource(): String          │
 * │  ├─ getTotalEntityCount(): long                      ├─ getRecordCount(): long           │
 * │  ├─ getTotalUnmatchedRecordCount(): long             ├─ getEntityCount(): long           │
 * │  └─ getDataSourceCounts(): List                      └─ getUnmatchedRecordCount(): long  │
 * │                                                                                          │
 * └──────────────────────────────────────────────────────────────────────────────────────────┘
 *
 * ┌──────────────────────────────────────────────────────────────────────────────────────────┐
 * │                        SUMMARY STATS HIERARCHY                                           │
 * ├──────────────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                          │
 * │  SzSummaryStats ────────────────1:* ────► SzSourceSummary ────1:* ───────────┐           │
 * │  └─ getSourceSummaries(): List            ├─ getDataSource(): String         │           │
 * │                                           ├─ getEntityCount(): long          │           │
 * │                                           ├─ getRecordCount(): long          │           │
 * │                                           ├─ getUnmatchedRecordCount(): long │           │
 * │                                           └─ getCrossSourceSummaries(): List │           │
 * │                                                      │                       │           │
 * │                               ┌──────────────────────┘                       │           │
 * │                               ▼                                              │           │
 * │                      SzCrossSourceSummary ◄──────────────────────────────────┘           │
 * │                      ├─ getDataSource(): String                                          │
 * │                      ├─ getVersusDataSource(): String                                    │
 * │                      ├─ getMatches(): List&lt;SzMatchCounts&gt;                                │
 * │                      ├─ getAmbiguousMatches(): List&lt;SzRelationCounts&gt;                    │
 * │                      ├─ getPossibleMatches(): List&lt;SzRelationCounts&gt;                     │
 * │                      ├─ getPossibleRelations(): List&lt;SzRelationCounts&gt;                   │
 * │                      └─ getDisclosedRelations(): List&lt;SzRelationCounts&gt;                  │
 * │                                      │                    │                              │
 * │                                      ▼                    ▼                              │
 * │                            SzMatchCounts          SzRelationCounts                       │
 * │                            ├─ getMatchKey()       ├─ getMatchKey()                       │
 * │                            ├─ getPrinciple()      ├─ getPrinciple()                      │
 * │                            ├─ getEntityCount()    ├─ getEntityCount()                    │
 * │                            └─ getRecordCount()    ├─ getRecordCount()                    │
 * │                                                   └─ getRelationCount()                  │
 * │                                                                                          │
 * └──────────────────────────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────────────────┐
 * │                         BREAKDOWN HIERARCHIES                                   │
 * ├─────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                 │
 * │  SzEntitySizeBreakdown ──────1:* ──► SzEntitySizeCount                          │
 * │  └─ getEntitySizeCounts(): List      ├─ getEntitySize(): int                    │
 * │                                      └─ getEntityCount(): long                  │
 * │                                                                                 │
 * │  SzEntityRelationsBreakdown ─1:* ──► SzEntityRelationsCount                     │
 * │  └─ getEntityRelationsCounts(): List ├─ getRelationsCount(): int                │
 * │                                      └─ getEntityCount(): long                  │
 * │                                                                                 │
 * └─────────────────────────────────────────────────────────────────────────────────┘
 *
 * ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │                          PAGING / DETAIL CLASSES                                                 │
 * ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                                  │
 * │  SzEntitiesPage ─────────────1:* ──► SzReportEntity ──────1:* ──────► SzReportRecord             │
 * │  ├─ getBound(): String               ├─ getEntityId(): long           ├─ getDataSource(): String │
 * │  ├─ getBoundType(): SzBoundType      ├─ getEntityName(): String       │─ getRecordId(): String   │
 * │  ├─ getPageSize(): int               ├─ getRecordCount(): Integer     ├─ getMatchKey(): String   │
 * │  ├─ getSampleSize(): Integer         ├─ getRelationCount(): Integer   └─ getPrinciple(): String  │
 * │  ├─ getMinimumValue(): Long          └─ getRecords(): List                                       │
 * │  ├─ getMaximumValue(): Long                                                                      │
 * │  ├─ getPageMinimumValue(): Long                                                                  │
 * │  ├─ getPageMaximumValue(): Long                                                                  │
 * │  ├─ getTotalEntityCount(): long                                                                  │
 * │  ├─ getBeforePageCount(): long                                                                   │
 * │  ├─ getAfterPageCount(): long                                                                    │
 * │  └─ getEntities(): List                                                                          │
 * │                                                                                                  │
 * │  SzRelationsPage ────────────1:* ──► SzReportRelation                                            │
 * │  ├─ getBound(): String               ├─ getEntity(): SzReportEntity ────────────────┐            │
 * │  ├─ getBoundType(): SzBoundType      ├─ getRelatedEntity(): SzReportEntity ─────────┤            │
 * │  ├─ getPageSize(): int               ├─ getRelationType(): SzRelationType           │            │
 * │  ├─ getSampleSize(): Integer         ├─ getMatchKey(): String                       │            │
 * │  ├─ getMinimumValue(): String        └─ getPrinciple(): String                      │            │
 * │  ├─ getMaximumValue(): String                       │                               │            │
 * │  ├─ getPageMinimumValue(): String                   │                               │            │
 * │  ├─ getPageMaximumValue(): String                   │    ┌──────────────────────────┘            │
 * │  ├─ getTotalRelationCount(): long                   │    │                                       │
 * │  ├─ getBeforePageCount(): long                      ▼    ▼                                       │
 * │  ├─ getAfterPageCount(): long                SzReportEntity                                      │
 * │  └─ getRelations(): List                                                                         │
 * │                                                                                                  │
 * └──────────────────────────────────────────────────────────────────────────────────────────────────┘
 *
 * ═══════════════════════════════════════════════════════════════════════════════════
 *                               RELATIONSHIP LEGEND
 * ═══════════════════════════════════════════════════════════════════════════════════
 *   ────1:*───►   One-to-many composition (contains collection of)
 *   ────────►     References / uses
 *   «enum»        Enumeration type
 * ═══════════════════════════════════════════════════════════════════════════════════
 * &lt;/pre&gt;
 *
 * &lt;h2&gt;Functional Groups&lt;/h2&gt;
 * &lt;p&gt;The model is organized into 4 main functional groups:&lt;/p&gt;
 * &lt;ol&gt;
 *   &lt;li&gt;&lt;strong&gt;Loaded Stats&lt;/strong&gt; - Basic record/entity counts per data source
 *       ({@link SzLoadedStats} → {@link SzSourceLoadedStats})&lt;/li&gt;
 *   &lt;li&gt;&lt;strong&gt;Summary Stats&lt;/strong&gt; - Cross-source matching statistics with
 *       match/relation counts broken down by match key and principle
 *       ({@link SzSummaryStats} → {@link SzSourceSummary} → {@link SzCrossSourceSummary}
 *       → {@link SzMatchCounts} / {@link SzRelationCounts})&lt;/li&gt;
 *   &lt;li&gt;&lt;strong&gt;Breakdowns&lt;/strong&gt; - Distribution histograms for entity sizes and
 *       relation counts ({@link SzEntitySizeBreakdown} → {@link SzEntitySizeCount},
 *       {@link SzEntityRelationsBreakdown} → {@link SzEntityRelationsCount})&lt;/li&gt;
 *   &lt;li&gt;&lt;strong&gt;Paging/Detail&lt;/strong&gt; - Paginated entity and relation results with
 *       full detail ({@link SzEntitiesPage} → {@link SzReportEntity} → {@link SzReportRecord},
 *       {@link SzRelationsPage} → {@link SzReportRelation})&lt;/li&gt;
 * &lt;/ol&gt;
 */
package com.senzing.datamart.reports.model;
