package com.senzing.datamart.handlers;

import com.senzing.datamart.model.*;
import com.senzing.listener.service.locking.ResourceKey;

import java.util.*;

import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.model.SzReportStatistic.*;
import static com.senzing.datamart.model.SzReportUpdate.builder;
import static com.senzing.datamart.model.SzMatchType.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Determines the delta between the old state of an entity and the new state of
 * the same entity (i.e.: the one with the same entity ID). In some cases the
 * old state or new state may be that the entity does not exist, however, the
 * both states should not be non-existent or else no difference exists.
 */
public class EntityDelta {
    /**
     * The lookup {@link Map} of {@link SzMatchType} keys to
     * {@link SzReportStatistic} values.
     */
    private static final Map<SzMatchType, SzReportStatistic> STATISTIC_LOOKUP = Map.of(AMBIGUOUS_MATCH,
            AMBIGUOUS_MATCH_COUNT, POSSIBLE_MATCH, POSSIBLE_MATCH_COUNT, DISCLOSED_RELATION, DISCLOSED_RELATION_COUNT,
            POSSIBLE_RELATION, POSSIBLE_RELATION_COUNT);

    /**
     * The {@link SzResolvedEntity} describing the old version of the entity, or
     * <code>null</code> if the entity did not previously exist in the data mart.
     */
    private SzResolvedEntity oldEntity;

    /**
     * The {@link SzResolvedEntity} describing the new version of the entity, or
     * <code>null</code> if the entity was deleted from the entity repository.
     */
    private SzResolvedEntity newEntity;

    /**
     * The difference between old and new for the number of records that the entity
     * is comprised of.
     */
    private int recordDelta;

    /**
     * The difference between old and new for the number of relationships the entity
     * has.
     */
    private int relationDelta;

    /**
     * The record-count deltas by data source between the old and the new data
     * source summaries.
     */
    private Map<String, Integer> dataSourceDeltas;

    /**
     * The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing the records that were added to the entity.
     */
    private Map<SzRecordKey, SzRecord> addedRecords;

    /**
     * The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing the records that have been created.
     */
    private Map<SzRecordKey, SzRecord> createdRecords;

    /**
     * The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing records that were removed from the entity.
     */
    private Map<SzRecordKey, SzRecord> removedRecords;

    /**
     * The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing the records that have been deleted.
     */
    private Map<SzRecordKey, SzRecord> deletedRecords;

    /**
     * The {@link SortedSet} of {@link ResourceKey} instances.
     */
    private SortedSet<ResourceKey> resourceKeys;

    /**
     * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
     * values describing the related entities that related to the entity, but
     * previously were not.
     */
    private Map<Long, SzRelatedEntity> addedRelations;

    /**
     * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
     * values describing the previously related entities that are no longer related
     * to the entity.
     */
    private Map<Long, SzRelatedEntity> removedRelations;

    /**
     * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
     * values describing the related entities for which a change in the relationship
     * occurred.
     */
    private Map<Long, SzRelatedEntity> changedRelations;

    /**
     * The {@link List} of {@link SzReportUpdate} instances detected from the
     * differences.
     */
    private List<SzReportUpdate> reportUpdates;

    /**
     * Constructs with the specified {@link SzResolvedEntity} instances to find the
     * deltas between.
     *
     * @param oldEntity The {@link SzResolvedEntity} describing the old entity
     *                  state.
     * @param newEntity The {@link SzResolvedEntity} describing the new entity
     *                  state.
     */
    public EntityDelta(SzResolvedEntity oldEntity, SzResolvedEntity newEntity) {
        if (oldEntity != null && newEntity != null && oldEntity.getEntityId() != newEntity.getEntityId()) {
            throw new IllegalArgumentException("The entities for which the differences are being discovered must "
                    + "describe the same entity at different points in time, rather than "
                    + "two different entities.  oldEntityId=[ " + oldEntity.getEntityId() + " ], newEntityId=[ "
                    + newEntity.getEntityId() + " ]");
        }

        // create the resource key set
        this.resourceKeys = new TreeSet<>();

        // set the entity fields
        this.oldEntity = oldEntity;
        this.newEntity = newEntity;

        // get the entity ID
        long entityId = getEntityId(this.oldEntity, this.newEntity);

        // get the old and new data sources
        Map<String, Integer> oldSourceSummary = getSourceSummary(oldEntity);
        Map<String, Integer> newSourceSummary = getSourceSummary(newEntity);

        this.dataSourceDeltas = new LinkedHashMap<>();
        oldSourceSummary.forEach((source, oldCount) -> {
            Integer newCount = newSourceSummary.get(source);
            if (newCount == null) {
                return;
            }
            this.dataSourceDeltas.put(source, -1 * oldCount);
        });
        newSourceSummary.forEach((source, newCount) -> {
            Integer oldCount = oldSourceSummary.get(source);
            if (oldCount == null) {
                this.dataSourceDeltas.put(source, newCount);
            } else {
                this.dataSourceDeltas.put(source, newCount - oldCount);
            }
        });
        this.dataSourceDeltas = Collections.unmodifiableMap(this.dataSourceDeltas);

        // get the data sources
        Set<String> oldDataSources = oldSourceSummary.keySet();
        Set<String> newDataSources = newSourceSummary.keySet();

        // determine the record count delta
        Map<SzRecordKey, SzRecord> oldRecords = getRecords(oldEntity);
        Map<SzRecordKey, SzRecord> newRecords = getRecords(newEntity);

        this.recordDelta = newRecords.size() - oldRecords.size();

        // determine the added records
        this.addedRecords = findAddedRecords(oldRecords, newRecords);

        this.addedRecords.values().forEach(record -> {
            this.resourceKeys.add(new ResourceKey("RECORD", record.getDataSource(), record.getRecordId()));
        });

        // determine the removed records
        this.removedRecords = findRemovedRecords(oldRecords, newRecords);

        this.removedRecords.values().forEach(record -> {
            this.resourceKeys.add(new ResourceKey("RECORD", record.getDataSource(), record.getRecordId()));
        });

        // determine the relationship count delta
        Map<Long, SzRelatedEntity> oldRelations = getRelatedEntities(oldEntity);
        Map<Long, SzRelatedEntity> newRelations = getRelatedEntities(newEntity);

        this.relationDelta = newRelations.size() - oldRelations.size();

        // determine the added relationships
        this.addedRelations = findAddedRelations(oldRelations, newRelations);

        this.addedRelations.values().forEach(relatedEntity -> {
            long relId = relatedEntity.getEntityId();
            long entityId1 = (entityId < relId) ? entityId : relId;
            long entityId2 = (entityId < relId) ? relId : entityId;
            this.resourceKeys.add(new ResourceKey("RELATIONSHIP", entityId1, entityId2));
        });

        // determine the removed relationships
        this.removedRelations = findRemovedRelations(oldRelations, newRelations);

        this.removedRelations.values().forEach(relatedEntity -> {
            long relId = relatedEntity.getEntityId();
            long entityId1 = (entityId < relId) ? entityId : relId;
            long entityId2 = (entityId < relId) ? relId : entityId;
            this.resourceKeys.add(new ResourceKey("RELATIONSHIP", entityId1, entityId2));
        });

        // determine the changed relationships
        this.changedRelations = findChangedRelations(oldDataSources, newDataSources, oldRelations, newRelations);

        this.changedRelations.values().forEach(relatedEntity -> {
            long relId = relatedEntity.getEntityId();
            long entityId1 = (entityId < relId) ? entityId : relId;
            long entityId2 = (entityId < relId) ? relId : entityId;
            this.resourceKeys.add(new ResourceKey("RELATIONSHIP", entityId1, entityId2));
        });

        // determine the ECB (Entity Count Breakdown) changes
        this.reportUpdates = new LinkedList<>();

        // check on the related source counts
        findRelatedSourceChanges(this.reportUpdates, oldEntity, newEntity);

        // check the entity size breakdown
        findEntitySizeChanges(this.reportUpdates, oldEntity, newEntity, oldRecords, newRecords);

        // check the entity relation breakdown
        findEntityRelationChanges(this.reportUpdates, oldEntity, newEntity, oldRelations, newRelations);

        // check the data source summary
        findSourceSummaryChanges(this.reportUpdates, oldEntity, newEntity, oldSourceSummary, newSourceSummary);

        // check the cross source summary
        findCrossMatchChanges(this.reportUpdates, oldEntity, newEntity, oldSourceSummary, newSourceSummary);

        // initialize the remaining members
        this.createdRecords = new LinkedHashMap<>();
        this.deletedRecords = new LinkedHashMap<>();

        // make the resource key set unmodifiable
        this.resourceKeys = Collections.unmodifiableSortedSet(this.resourceKeys);
    }

    /**
     * Gets the entity ID for the entity that the delta was performed on.
     *
     * @return The entity ID for the entity that the delta was performed on.
     */
    public long getEntityId() {
        return getEntityId(this.getOldEntity(), this.getNewEntity());
    }

    /**
     * Gets the {@link SzResolvedEntity} describing the old entity.
     *
     * @return The {@link SzResolvedEntity} describing the old entity.
     */
    public SzResolvedEntity getOldEntity() {
        return this.oldEntity;
    }

    /**
     * Gets the {@link SzResolvedEntity} describing the new entity.
     *
     * @return The {@link SzResolvedEntity} describing the new entity.
     */
    public SzResolvedEntity getNewEntity() {
        return this.newEntity;
    }

    /**
     * Gets the {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing the records for the old version of the entity. If the entity did
     * not previously exist (i.e.: the old entity is <code>null</code>) then this
     * returns an empty {@link Map}.
     *
     * @return The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord}
     *         values describing the records for the old version of the entity, or
     *         an empty {@link Map} if the entity did not previously exist.
     */
    public Map<SzRecordKey, SzRecord> getOldRecords() {
        SzResolvedEntity entity = this.getOldEntity();
        return (entity == null) ? Collections.emptyMap() : entity.getRecords();
    }

    /**
     * Gets the {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing the records for the new version of the entity. If the entity was
     * deleted (i.e.: the new entity is <code>null</code>) then this returns an
     * empty {@link Map}.
     *
     * @return The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord}
     *         values describing the records for the new version of the entity, or
     *         an empty {@link Map} if the entity was deleted.
     */
    public Map<SzRecordKey, SzRecord> getNewRecords() {
        SzResolvedEntity entity = this.getNewEntity();
        return (entity == null) ? Collections.emptyMap() : entity.getRecords();
    }

    /**
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} describing the record counts per data source for the old
     * version of the entity. If the entity did not previously exist (i.e.: the old
     * entity is <code>null</code>) then this returns an empty {@link Map}.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} describing the record counts per data source for the
     *         old version of the entity.
     */
    public Map<String, Integer> getOldSourceSummary() {
        return getSourceSummary(this.getOldEntity());
    }

    /**
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} describing the record counts per data source for the new
     * version of the entity. If the entity was deleted (i.e.: the new entity is
     * <code>null</code>) then this returns an empty {@link Map}.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} describing the record counts per data source for the
     *         new version of the entity.
     */
    public Map<String, Integer> getNewSourceSummary() {
        return getSourceSummary(this.getNewEntity());
    }

    /**
     * Gets the {@link SortedMap} of {@link SzMatchType} keys to {@link SortedSet}
     * values containing data source codes for the data sources of the records in
     * all entities related to the old entity. If the entity did not previously
     * exist (i.e.: the old entity is <code>null</code>) then this returns an empty
     * {@link Set}.
     *
     * @return The {@link Set} of {@link String} data source codes for the data
     *         sources of the records in all entities related to the old entity.
     */
    public SortedMap<SourceRelationKey, SortedSet<String>> getOldRelatedSources() {
        return getRelatedSources(this.getOldEntity());
    }

    /**
     * Gets the {@link Set} of {@link String} data source codes for the data sources
     * of the records in all entities related to the new entity. If the entity no
     * longer exists (i.e.: the new entity is <code>null</code>) then this returns
     * an empty {@link Set}.
     *
     * @return The {@link Set} of {@link String} data source codes for the data
     *         sources of the records in all entities related to the new entity.
     */
    public SortedMap<SourceRelationKey, SortedSet<String>> getNewRelatedSources() {
        return getRelatedSources(this.getNewEntity());
    }

    /**
     * Gets the {@link Map} of {@link Long} entity ID keys to
     * {@link SzRelatedEntity} instances describing the entities previously related
     * to this entity. If the entity previously did not exist (i.e.: the old entity
     * is <code>null</code>) then this returns an empty {@link Map}.
     *
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} instances describing the entities previously
     *         related to this entity.
     */
    public Map<Long, SzRelatedEntity> getOldRelatedEntities() {
        return getRelatedEntities(this.getOldEntity());
    }

    /**
     * Gets the {@link Map} of {@link Long} entity ID keys to
     * {@link SzRelatedEntity} instances describing the currently related entities
     * to this entity. If the entity no longer exists (i.e.: the new entity is
     * <code>null</code>) then this returns an empty {@link Map}.
     *
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} instances describing the entities currently
     *         related to this entity.
     */
    public Map<Long, SzRelatedEntity> getNewRelatedEntities() {
        return getRelatedEntities(this.getNewEntity());
    }

    /**
     * Gets the delta for the record count.
     *
     * @return The delta for the record count.
     */
    public int getRecordDelta() {
        return this.recordDelta;
    }

    /**
     * Gets the delta for the relationship count.
     *
     * @return The delta for the relationship count.
     */
    public int getRelationDelta() {
        return this.relationDelta;
    }

    /**
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} values describing the deltas in the record counts by data
     * source between the old and the new version of the entity.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} values describing the deltas in the record counts by
     *         data source between the old and the new version of the entity.
     */
    public Map<String, Integer> getDataSourceDeltas() {
        return this.dataSourceDeltas;
    }

    /**
     * Gets the {@link Set} of {@link SzRecord} instances describing the records
     * that were added at the entity.
     *
     * @return The {@link Set} of {@link SzRecord} instances describing the records
     *         that were added ot the entity.
     */
    public Map<SzRecordKey, SzRecord> getAddedRecords() {
        return Collections.unmodifiableMap(this.addedRecords);
    }

    /**
     * Indicates that the specified record that was added to the entity had to be
     * created new in the data mart.
     *
     * @param record The {@link SzRecord} describing the record that was newly
     *               created.
     * @throws NullPointerException     If the specified {@link SzRecord} is
     *                                  <code>null</code>.
     * @throws IllegalArgumentException If the specified {@link SzRecord} is not one
     *                                  of the {@linkplain #getAddedRecords() added
     *                                  records} or has already been marked as
     *                                  created.
     */
    public void createdRecord(SzRecord record) {
        logDebug("RECORD CREATED BY ENTITY " + this.getEntityId() + ": " + record);
        Objects.requireNonNull(record, "The record cannot be null");
        if (!this.getAddedRecords().containsKey(record.getRecordKey())) {
            throw new IllegalArgumentException("The specified record is not one of the added records.  record=[ "
                    + record + " ], allowed=[ " + this.getAddedRecords() + " ]");
        }
        if (this.createdRecords.containsKey(record.getRecordKey())) {
            throw new IllegalArgumentException(
                    "The specified record has already been marked as created.  record=[ " + record + " ]");
        }
        this.createdRecords.put(record.getRecordKey(), record);

        String source = record.getDataSource();
        long entityId = this.getEntityId();
        this.reportUpdates.add(builder(DATA_SOURCE_SUMMARY, ENTITY_COUNT, source, source, entityId).records(1).build());
    }

    /**
     * Gets the {@link Map} of {@link SzRecordKey} keys to {@link SzRecord} values
     * describing the records that were removed from the entity.
     *
     * @return The {@link Map} of {@link SzRecordKey} keys to {@link SzRecord}
     *         values describing the records that were removed from the entity.
     */
    public Map<SzRecordKey, SzRecord> getRemovedRecords() {
        return Collections.unmodifiableMap(this.removedRecords);
    }

    /**
     * Indicates that the specified record that was added to the entity had to be
     * created new in the data mart.
     *
     * @param record The {@link SzRecord} describing the record that was newly
     *               created.
     * @throws NullPointerException     If the specified {@link SzRecord} is
     *                                  <code>null</code>.
     * @throws IllegalArgumentException If the specified {@link SzRecord} is not one
     *                                  of the {@linkplain #getAddedRecords() added
     *                                  records} or has already been marked as
     *                                  created.
     */
    public void orphanedRecord(SzRecord record) {
        logDebug("RECORD ORPHANED BY ENTITY " + this.getEntityId() + ": " + record);
        Objects.requireNonNull(record, "The record cannot be null");
        if (!this.getRemovedRecords().containsKey(record.getRecordKey())) {
            throw new IllegalArgumentException("The specified record is not one of the removed records.  record=[ "
                    + record + " ], allowed=[ " + this.getRemovedRecords() + " ]");
        }
        if (this.deletedRecords.containsKey(record.getRecordKey())) {
            throw new IllegalArgumentException(
                    "The specified record has already been marked as deleted.  record=[ " + record + " ]");
        }

        String source = record.getDataSource();
        long entityId = this.getEntityId();

        this.deletedRecords.put(record.getRecordKey(), record);

        reportUpdates.add(builder(DATA_SOURCE_SUMMARY, ENTITY_COUNT, source, source, entityId).records(-1).build());
    }

    /**
     * Gets the {@link Map} of {@link Long} entity ID keys to
     * {@link SzRelatedEntity} values describing the related entities that are now
     * related to the entity, but previously were not related.
     *
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the related entities that
     *         are now related to the entity, but previously were not related.
     */
    public Map<Long, SzRelatedEntity> getAddedRelations() {
        return Collections.unmodifiableMap(this.addedRelations);
    }

    /**
     * Gets the {@link Map} of {@link Long} entity ID keys to
     * {@link SzRelatedEntity} values describing the related entities for which the
     * relationship has changed in some way (e.g.: new data sources for this entity
     * or the other).
     *
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the related entities that
     *         are no longer related to the entity, but previously were related.
     */
    public Map<Long, SzRelatedEntity> getChangedRelations() {
        return Collections.unmodifiableMap(this.changedRelations);
    }

    /**
     * Marks the relationship to the specified related entity as having been stored
     * in the database. The relationship may have been created or updated or found
     * to already exist.
     *
     * @param entityId          The entity ID of the first entity in the
     *                          relationship.
     * @param relatedId         The entity ID of the second entity in the
     *                          relationship.
     * @param oldMatchType      The previous {@link SzMatchType} stored in the data
     *                          mart database, or <code>null</code> if not
     *                          previously stored.
     * @param oldMatchKey       The previous match key stored in the data mart
     *                          database for the relationship, or <code>null</code>
     *                          if not previously stored.
     * @param oldPrinciple      The previous principle (entity resolution rule code)
     *                          stored in the data mart database, or
     *                          <code>null</code> if not previously stored.
     * @param oldSourceSummary  The {@link Map} of {@link String} data source keys
     *                          to {@link Integer} values providing the data source
     *                          record summary of the entity identified by
     *                          <code>entityId</code> previously associated with the
     *                          relationship, or <code>null</code> (or empty) if the
     *                          relationship was newly created.
     * @param oldRelatedSummary The {@link Map} of {@link String} data source keys
     *                          to {@link Integer} values providing the data source
     *                          record summary of the entity identified by
     *                          <code>relatedId</code> previously associated with
     *                          the relationship, or <code>null</code> (or empty) if
     *                          the relationship was newly created.
     * @throws IllegalArgumentException If the specified entity ID's are not
     *                                  different or neither matches the entity ID
     *                                  for this instance.
     */
    public void trackStoredRelationship(long entityId, long relatedId, SzMatchType oldMatchType, String oldMatchKey, String oldPrinciple, Map<String, Integer> oldSourceSummary, Map<String, Integer> oldRelatedSummary) {
        // get our entity ID
        long myEntityId = this.getEntityId();

        // normalize possibly-null arguments
        if (oldSourceSummary == null) {
            oldSourceSummary = Collections.emptyMap();
        }
        if (oldRelatedSummary == null) {
            oldRelatedSummary = Collections.emptyMap();
        }

        // check that our entity ID is one of the specified entity ID's
        if (entityId != myEntityId && relatedId != myEntityId) {
            throw new IllegalArgumentException(
                    "Neither of the specified entity ID's match the entity ID for this " + "instance.  entityId=[ "
                            + entityId + " ], relatedId=[ " + relatedId + " ], expectedId=[ " + myEntityId + " ]");
        }
        // check both entity ID's are not equal
        if (entityId == relatedId) {
            throw new IllegalArgumentException(
                    "Both the entity ID and the related entity ID cannot be the same: " + entityId);
        }

        // normalize everything with respect to this entity's perspective
        if (myEntityId == relatedId) {
            relatedId = entityId;
            entityId = myEntityId;
            Map<String, Integer> temp = oldSourceSummary;
            oldSourceSummary = oldRelatedSummary;
            oldRelatedSummary = temp;
        }

        // now check that the related entity ID is related to the entity
        if (!this.getAddedRelations().containsKey(relatedId) && !this.getChangedRelations().containsKey(relatedId)) {
            throw new IllegalArgumentException("Cannot store the relationship to an entity for which the "
                    + "relationship was not added or changed.  relatedId=[ " + relatedId + " ], added=[ "
                    + this.getAddedRelations().keySet() + " ], changed=[ " + this.getChangedRelations().keySet()
                    + " ]");
        }

        logDebug("ENTITY " + entityId + " OLD SOURCE SUMMARY TO ENTITY " + relatedId + ": ", oldSourceSummary,
                oldRelatedSummary);

        // determine the previous cross-source pairs
        Map<List<String>, RelationshipCounts> oldRelCounts = countRelationships(oldSourceSummary, oldRelatedSummary);

        logDebug("ENTITY " + entityId + " OLD REL COUNTS TO ENTITY " + relatedId + ": ", oldRelCounts);

        // get the related entities
        Map<Long, SzRelatedEntity> relatedEntityMap = this.getNewRelatedEntities();
        SzRelatedEntity related = relatedEntityMap.get(relatedId);

        // get the data source summary
        Map<String, Integer> newSourceSummary = this.getNewSourceSummary();
        Map<String, Integer> newRelatedSummary = related.getSourceSummary();

        // determine the new cross-source pairs
        Map<List<String>, RelationshipCounts> newRelCounts = countRelationships(newSourceSummary, newRelatedSummary);

        // get the current match type
        SzMatchType matchType = related.getMatchType();
        String principle = related.getPrinciple();
        String matchKey = related.getMatchKey();

        // create final ID's to use in lambda expression
        final long fromId = entityId;
        final long toId = relatedId;

        logDebug("ENTITY " + fromId + " CHECKING RELATION TO " + toId + " WITH: matchType=[ " + matchType
                + " ] AND oldMatchType=[ " + oldMatchType + " ]");

        // check if we need to decrement counts for an old match type, principle
        // and/or match key
        Set<String> decrementStats = decrementStatVariants(oldMatchType, matchType, oldPrinciple, principle,
                oldMatchKey, matchKey);

        decrementStats.forEach(statistic -> {
            logDebug("ENTITY " + fromId + " HANDLING RELATION TO " + toId + " WITH: " + oldRelCounts);

            // handle decrementing the counts accordingly
            oldRelCounts.forEach((sourcePair, counts) -> {
                String source1 = sourcePair.get(0);
                String source2 = sourcePair.get(1);

                boolean sameSource = source1.equals(source2);

                SzReportCode reportCode = (sameSource) ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

                logDebug("A) ENTITY " + fromId + ": UNTRACKING RELATION TO " + toId + " for " + reportCode + ":["
                        + statistic + "]:" + source1 + ":" + source2);

                // decrement the relationship count for this relationship for each stat
                reportUpdates.add(builder(reportCode, statistic, source1, source2, fromId, toId).relations(-1).build());
            });
        });

        // check if we need to increment counts for the new match type,
        // principle and/or match key
        Set<String> incrementStats = incrementStatVariants(oldMatchType, matchType, oldPrinciple, principle,
                oldMatchKey, matchKey);

        incrementStats.forEach(statistic -> {
            // handle incrementing the counts accordingly
            newRelCounts.forEach((sourcePair, counts) -> {
                String source1 = sourcePair.get(0);
                String source2 = sourcePair.get(1);
                boolean sameSource = source1.equals(source2);

                SzReportCode reportCode = (sameSource) ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

                logDebug("B) ENTITY " + fromId + ": TRACKING RELATION TO " + toId + " for " + reportCode + ":["
                        + statistic + "]:" + source1 + ":" + source2);

                // increment the relationship count for this relationship for each stat
                reportUpdates.add(builder(reportCode, statistic, source1, source2, fromId, toId).relations(1).build());
            });
        });

        // find the stats for which we need to compute the deltas
        Set<String> deltaStats = deltaStatVariants(oldMatchType, matchType, oldPrinciple, principle, oldMatchKey,
                matchKey);

        // iterate over the statistics
        deltaStats.forEach(statistic -> {
            Set<List<String>> allSourcePairs = new LinkedHashSet<>();
            allSourcePairs.addAll(newRelCounts.keySet());
            allSourcePairs.addAll(oldRelCounts.keySet());

            allSourcePairs.forEach((sourcePair) -> {
                RelationshipCounts newCounts = newRelCounts.get(sourcePair);
                RelationshipCounts oldCounts = oldRelCounts.get(sourcePair);

                String source1 = sourcePair.get(0);
                String source2 = sourcePair.get(1);

                boolean sameSource = source1.equals(source2);

                SzReportCode reportCode = (sameSource) ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

                // check if an old relationship no longer exists
                if (oldCounts != null && newCounts == null) {
                    logDebug("C) ENTITY " + fromId + ": UNTRACKING RELATION TO " + toId + " for " + reportCode + ":["
                            + statistic + "]:" + source1 + ":" + source2);

                    // decrement the relationship count for this relationship
                    reportUpdates
                            .add(builder(reportCode, statistic, source1, source2, fromId, toId).relations(-1).build());
                }

                // check if a new relationship exists that previously did not
                if (oldCounts == null && newCounts != null) {
                    logDebug("D) ENTITY " + fromId + ": TRACKING RELATION TO " + toId + " for " + reportCode + ":["
                            + statistic + "]:" + source1 + ":" + source2);

                    // increment the relationship count for this relationship
                    reportUpdates
                            .add(builder(reportCode, statistic, source1, source2, fromId, toId).relations(1).build());
                }
            });
        });
    }

    /**
     * Gets the {@link Map} of {@link Long} entity ID keys to
     * {@link SzRelatedEntity} values describing the related entities that are no
     * longer related to the entity, but previously were related.
     *
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the related entities that
     *         are no longer related to the entity, but previously were related.
     */
    public Map<Long, SzRelatedEntity> getRemovedRelations() {
        return Collections.unmodifiableMap(this.removedRelations);
    }

    /**
     * Marks the relationship to the specified related entity as having been deleted
     * or found to be already deleted from the database. The relationship must have
     * previously existed.
     *
     * @param entityId          The entity ID of the first entity in the
     *                          relationship.
     * @param relatedId         The entity ID of the second entity in the
     *                          relationship.
     * @param oldMatchType      The non-null previous {@link SzMatchType} stored in
     *                          the data mart database.
     * @param oldMatchKey       The previous match key stored in the data mart
     *                          database for the relationship, or <code>null</code>
     *                          if not previously stored.
     * @param oldPrinciple      The previous principle (entity resolution rule code)
     *                          stored in the data mart database, or
     *                          <code>null</code> if not previously stored.
     * @param oldSourceSummary  The non-null {@link Map} of {@link String} data
     *                          source keys to {@link Integer} values providing the
     *                          data source record summary of the entity identified
     *                          by <code>entityId</code> previously associated with
     *                          the relationship.
     * @param oldRelatedSummary The non-null {@link Map} of {@link String} data
     *                          source keys to {@link Integer} values providing the
     *                          data source record summary of the entity identified
     *                          by <code>relatedId</code> previously associated with
     *                          the relationship.
     * @throws NullPointerException     If a parameter is unexpectedly
     *                                  <code>null</code>.
     * @throws IllegalArgumentException If the specified entity ID's are not
     *                                  different or neither matches the entity ID
     *                                  for this instance.
     */
    public void trackDeletedRelationship(long entityId, long relatedId, SzMatchType oldMatchType, String oldMatchKey, String oldPrinciple, Map<String, Integer> oldSourceSummary, Map<String, Integer> oldRelatedSummary) throws NullPointerException, IllegalArgumentException {
        // get our entity ID
        long myEntityId = this.getEntityId();

        Objects.requireNonNull(oldSourceSummary, "Previous source summary cannot be null if deleted.  " + "entityId=[ "
                + entityId + " ], relatedId=[ " + relatedId + " ]");

        Objects.requireNonNull(oldRelatedSummary, "Previous related source summary cannot be null if deleted.  "
                + "entityId=[ " + entityId + " ], relatedId=[ " + relatedId + " ]");

        // check that our entity ID is one of the specified entity ID's
        if (entityId != myEntityId && relatedId != myEntityId) {
            throw new IllegalArgumentException(
                    "Neither of the specified entity ID's match the entity ID for this " + "instance.  entityId=[ "
                            + entityId + " ], relatedId=[ " + relatedId + " ], expectedId=[ " + myEntityId + " ]");
        }

        Objects.requireNonNull(oldMatchType, "Previous match type cannot be null if deleted.  entityId=[ " + entityId
                + " ], relatedId=[ " + relatedId + " ]");

        Objects.requireNonNull(oldMatchKey, "Previous match key cannot be null if deleted.  entityId=[ " + entityId
                + " ], relatedId=[ " + relatedId + " ]");

        Objects.requireNonNull(oldPrinciple, "Previous principle cannot be null if deleted.  entityId=[ " + entityId
                + " ], relatedId=[ " + relatedId + " ]");

        // check both entity ID's are not equal
        if (entityId == relatedId) {
            throw new IllegalArgumentException(
                    "Both the entity ID and the related entity ID cannot be the same: " + entityId);
        }

        // normalize everything with respect to this entity's perspective
        if (myEntityId == relatedId) {
            relatedId = entityId;
            entityId = myEntityId;
            Map<String, Integer> temp = oldSourceSummary;
            oldSourceSummary = oldRelatedSummary;
            oldRelatedSummary = temp;
        }

        // now check that the related entity ID is related to the entity
        if (!this.getRemovedRelations().containsKey(relatedId)) {
            throw new IllegalArgumentException("Cannot delete the relationship to an entity for which the "
                    + "relationship was not removed.  relatedId=[ " + relatedId + " ], removed=[ "
                    + this.getRemovedRelations().keySet() + " ]");
        }

        // determine the previous cross-source pairs
        Map<List<String>, RelationshipCounts> oldRelCounts = countRelationships(oldSourceSummary, oldRelatedSummary);

        // get the statistic
        SzReportStatistic statistic = STATISTIC_LOOKUP.get(oldMatchType);

        // create final ID's to use in lambda expression
        final long fromId = entityId;
        final long toId = relatedId;

        // handle decrementing the counts accordingly
        oldRelCounts.forEach((sourcePair, counts) -> {
            String source2 = sourcePair.get(1);
            String source1 = sourcePair.get(0);

            boolean sameSource = source1.equals(source2);

            SzReportCode reportCode = (sameSource) ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

            statisticVariants(statistic, oldPrinciple, oldMatchKey).forEach(stat -> {

                logDebug("E) ENTITY " + fromId + ": UNTRACKING RELATION TO " + toId + " for " + reportCode + ":" + stat
                        + ":" + source1 + ":" + source2);

                // decrement the relationship count for this relationship
                reportUpdates.add(builder(reportCode, stat, source1, source2, fromId, toId).relations(-1).build());
            });
        });
    }

    /**
     * Get all required report updates that have been detected so far.
     *
     * @return The {@link List} of {@link SzReportUpdate} instances describing the
     *         updates detected thus far.
     */
    public List<SzReportUpdate> getReportUpdates() {
        return Collections.unmodifiableList(this.reportUpdates);
    }

    /**
     * Gets the {@link Map} of {@link String} data source codes to {@link Integer}
     * record counts describing the data source summary for the specified
     * {@link SzEntity}. If the specified {@link SzEntity} is <code>null</code> then
     * this returns an empty {@link Map}.
     *
     * @param entity The {@link SzEntity} from which to obtain the data source
     *               summary.
     * @return The {@link Map} of {@link String} data source codes to
     *         {@link Integer} record counts describing the data source summary for
     *         the specified {@link SzEntity}, or an empty {@link Map} if the
     *         specified {@link SzEntity} is <code>null</code>.
     */
    private static Map<String, Integer> getSourceSummary(SzEntity entity) {
        return (entity == null) ? Collections.emptyMap() : entity.getSourceSummary();

    }

    /**
     * Gets the {@link SortedMap} of {@link SourceRelationKey} keys to
     * {@link SortedSet} values containing {@link String} data source codes
     * describing the data sources related by the respective
     * {@link SourceRelationKey}. If the specified {@link SzEntity} is
     * <code>null</code> then this returns an empty {@link SortedMap}.
     *
     * @param entity The {@link SzResolvedEntity} from which to obtain the related
     *               sources.
     *
     * @return The {@link SortedMap} of {@link SourceRelationKey} keys to
     *         {@link SortedSet} values containing {@link String} data source codes
     *         describing the data sources related by the respective
     *         {@link SourceRelationKey}, or an empty {@link SortedMap} if the
     *         specified {@link SzResolvedEntity} is <code>null</code>.
     */
    private static SortedMap<SourceRelationKey, SortedSet<String>> getRelatedSources(SzResolvedEntity entity) {
        SortedMap<SourceRelationKey, SortedSet<String>> result = new TreeMap<>();
        if (entity == null) {
            return result;
        }
        entity.getRelatedEntities().values().forEach(related -> {
            SzMatchType matchType = related.getMatchType();
            String matchKey = related.getMatchKey();
            String principle = related.getPrinciple();

            SourceRelationKey.variants(matchType, matchKey, principle).forEach(key -> {
                SortedSet<String> sources = result.get(key);
                if (sources == null) {
                    sources = new TreeSet<>();
                    result.put(key, sources);
                }
                sources.addAll(related.getSourceSummary().keySet());
            });
        });
        return result;
    }

    /**
     * Gets the {@link Set} of {@link SzRecord} instances describing the records for
     * the specified {@link SzEntity}. If the specified {@link SzEntity} is
     * <code>null</code> then this returns an empty {@link Map}.
     *
     * @param entity The {@link SzEntity} from which obtain the data sources.
     * @return The {@link Set} of {@link SzRecord} instances describing the records
     *         for the specified {@link SzEntity}, or an empty {@link Set} if the
     *         specified {@link SzEntity} is <code>null</code>.
     */
    private static Map<SzRecordKey, SzRecord> getRecords(SzEntity entity) {
        return (entity == null) ? Collections.emptyMap() : new LinkedHashMap<>(entity.getRecords());
    }

    /**
     * Gets the entity ID associated with the specified old and new {@link SzEntity}
     * instances, at least one of which must <b>not</b> be <code>null</code>. It is
     * assumed that both describe the same entity at different points in time if
     * both are non-null.
     *
     * @param oldEntity The {@link SzEntity} describing the old state of the entity,
     *                  or <code>null</code> if the entity did not previously exist
     *                  in the data mart.
     * @param newEntity The {@link SzEntity} describing the new state of the entity,
     *                  or <code>null</code> if the entity no longer exists in the
     *                  entity repository.
     * @return The associated entity ID.
     * @throws IllegalArgumentException If both parameters are <code>null</code>.
     */
    private static long getEntityId(SzEntity oldEntity, SzEntity newEntity) {
        if (oldEntity != null) {
            return oldEntity.getEntityId();
        }
        if (newEntity != null) {
            return newEntity.getEntityId();
        }
        throw new IllegalArgumentException("Both arguments cannot be null.");
    }

    /**
     * Gets the {@link Map} of {@link Long} entity ID keys to
     * {@link SzRelatedEntity} values describing the related entities for the
     * specified {@link SzEntity}. If the specified {@link SzEntity} is
     * <code>null</code> then an empty {@link Map} is returned.
     *
     * @param entity The {@link SzEntity} for which the related entities are being
     *               requested.
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the related entities for
     *         the specified {@link SzEntity}, or an empty {@link Map} if the
     *         specified {@link SzEntity} is <code>null</code>.
     */
    private static Map<Long, SzRelatedEntity> getRelatedEntities(SzResolvedEntity entity) {
        return (entity == null) ? Collections.emptyMap() : entity.getRelatedEntities();
    }

    /**
     * Determines which records were added to the entity given the specified
     * {@link Set}'s of old and new records.
     *
     * @param oldRecords The {@link Set} of {@link SzRecord} instances describing
     *                   the records that were previously recorded in the entity.
     * @param newRecords The {@link Set} of {@link SzRecord} instances describing
     *                   the records that are now part of the entity.
     * @return The {@link Set} of {@link SzRecord} instances that are new to the
     *         entity.
     */
    private static Map<SzRecordKey, SzRecord> findAddedRecords(Map<SzRecordKey, SzRecord> oldRecords, Map<SzRecordKey, SzRecord> newRecords) {
        Map<SzRecordKey, SzRecord> records = new LinkedHashMap<>();
        newRecords.values().forEach(record -> {
            if (!oldRecords.containsKey(record.getRecordKey())) {
                records.put(record.getRecordKey(), record);
            }
        });
        return records;
    }

    /**
     * Determines which records were removed from the entity given the specified
     * {@link Set}'s of old and new records.
     *
     * @param oldRecords The {@link Set} of {@link SzRecord} instances describing
     *                   the records that were previously recorded in the entity.
     * @param newRecords The {@link Set} of {@link SzRecord} instances describing
     *                   the records that are now part of the entity.
     * @return The {@link Set} of {@link SzRecord} instances that were previously
     *         part of the entity, but are no longer part of the entity.
     */
    private static Map<SzRecordKey, SzRecord> findRemovedRecords(Map<SzRecordKey, SzRecord> oldRecords, Map<SzRecordKey, SzRecord> newRecords) {
        Map<SzRecordKey, SzRecord> records = new LinkedHashMap<>();
        oldRecords.values().forEach(record -> {
            if (!newRecords.containsKey(record.getRecordKey())) {
                records.put(record.getRecordKey(), record);
            }
        });
        return records;
    }

    /**
     * Determines which relationships were added to the entity given the specified
     * {@link Map}'s describing the old and new relationships.
     *
     * @param oldRelations The {@link Map} of {@link Long} entity ID keys to
     *                     {@link SzRelatedEntity} instances describing the
     *                     relationships that entity previous had.
     * @param newRelations The {@link Map} of {@link Long} entity ID keys to
     *                     {@link SzRelatedEntity} instances describing the
     *                     relationships that the entity now has.
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the newly added
     *         relationships.
     */
    private static Map<Long, SzRelatedEntity> findAddedRelations(Map<Long, SzRelatedEntity> oldRelations, Map<Long, SzRelatedEntity> newRelations) {
        // determine the added relationships
        Map<Long, SzRelatedEntity> relations = new LinkedHashMap<>();
        newRelations.forEach((entityId, entity) -> {
            if (!oldRelations.containsKey(entityId)) {
                relations.put(entity.getEntityId(), entity);
            }
        });
        return relations;
    }

    /**
     * Determines which relationships were removed from the entity given the
     * specified {@link Map}'s describing the old and new relationships.
     *
     * @param oldRelations The {@link Map} of {@link Long} entity ID keys to
     *                     {@link SzRelatedEntity} instances describing the
     *                     relationships that entity previous had.
     * @param newRelations The {@link Map} of {@link Long} entity ID keys to
     *                     {@link SzRelatedEntity} instances describing the
     *                     relationships that the entity now has.
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the removed relationships.
     */
    private static Map<Long, SzRelatedEntity> findRemovedRelations(Map<Long, SzRelatedEntity> oldRelations, Map<Long, SzRelatedEntity> newRelations) {
        // determine the added relationships
        Map<Long, SzRelatedEntity> relations = new LinkedHashMap<>();

        // determine the removed relationships
        oldRelations.forEach((entityId, entity) -> {
            if (!newRelations.containsKey(entityId)) {
                relations.put(entity.getEntityId(), entity);
            }
        });

        return relations;
    }

    /**
     * Determines which relationships were changed for the entity given the
     * specified {@link Map}'s describing the old and new relationships. All the
     * relationships in the returned {@link Map} were present in the {@link Map} of
     * old relations, but also exist and are different in the new relations. In some
     * cases, the only difference is the data sources have changed for the entity.
     *
     * @param oldDataSources The {@link Set} of {@link String} data source codes
     *                       describing the data sources that the entity previously
     *                       had.
     * @param newDataSources The {@link Set} of {@link String} data source codes
     *                       describing the data sources that the entity now has.
     * @param oldRelations   The {@link Map} of {@link Long} entity ID keys to
     *                       {@link SzRelatedEntity} instances describing the
     *                       relationships that entity previous had.
     * @param newRelations   The {@link Map} of {@link Long} entity ID keys to
     *                       {@link SzRelatedEntity} instances describing the
     *                       relationships that the entity now has.
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzRelatedEntity} values describing the entity relationships
     *         that have changed.
     */
    private static Map<Long, SzRelatedEntity> findChangedRelations(Set<String> oldDataSources, Set<String> newDataSources, Map<Long, SzRelatedEntity> oldRelations, Map<Long, SzRelatedEntity> newRelations) {
        boolean dataSourcesChanged = (!oldDataSources.equals(newDataSources));

        Map<Long, SzRelatedEntity> relations = new LinkedHashMap<>();

        newRelations.forEach((entityId, newRelated) -> {
            // check if the relationship previously existed
            SzRelatedEntity oldRelated = oldRelations.get(entityId);
            if (oldRelated == null) {
                return;
            }

            // check if the data sources changed for this entity
            if (dataSourcesChanged) {
                relations.put(newRelated.getEntityId(), newRelated);
            }

            // check if the data sources changed for the other entity
            Map<String, Integer> oldRelatedSourceBreakdown = oldRelated.getSourceSummary();
            Map<String, Integer> newRelatedSourceBreakdown = newRelated.getSourceSummary();

            Set<String> oldRelatedSources = oldRelatedSourceBreakdown.keySet();
            Set<String> newRelatedSources = newRelatedSourceBreakdown.keySet();

            if (!oldRelatedSources.equals(newRelatedSources)) {
                relations.put(newRelated.getEntityId(), newRelated);
            }

            // check if other aspects of the related entity have changed
            if ((oldRelated.getMatchLevel() != newRelated.getMatchLevel())
                    || (oldRelated.getMatchType() != newRelated.getMatchType())
                    || (!oldRelated.getMatchKey().equals(newRelated.getMatchKey()))
                    || (!oldRelated.getPrinciple().equals(newRelated.getPrinciple()))) {
                relations.put(newRelated.getEntityId(), newRelated);
            }
        });

        return relations;
    }

    /**
     * Determines which relationships were changed for the entity given the
     * specified {@link Map}'s describing the old and new relationships. All the
     * relationships in the returned {@link Map} were present in the {@link Map} of
     * old relations, but also exist and are different in the new relations. In some
     * cases, the only difference is the data sources have changed for the entity.
     *
     * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances.
     * @param oldEntity     The {@link SzResolvedEntity} describing the old entity.
     * @param newEntity     The {@link SzResolvedEntity} describing the new entity.
     */
    private static void findRelatedSourceChanges(List<SzReportUpdate> reportUpdates, SzResolvedEntity oldEntity, SzResolvedEntity newEntity) {
        long entityId = getEntityId(oldEntity, newEntity);

        // now check get the current and previous data sources for this entity
        Map<String, Integer> oldSummary = getSourceSummary(oldEntity);
        Map<String, Integer> newSummary = getSourceSummary(newEntity);

        // get the related sources
        SortedMap<SourceRelationKey, SortedSet<String>> oldRelSources = getRelatedSources(oldEntity);

        SortedMap<SourceRelationKey, SortedSet<String>> newRelSources = getRelatedSources(newEntity);

        // get the cross relations
        Map<CrossRelationKey, Integer> oldCrossRelations = crossSourceRelations(oldSummary, oldRelSources);

        Map<CrossRelationKey, Integer> newCrossRelations = crossSourceRelations(newSummary, newRelSources);

        // iterate over the old cross relations
        oldCrossRelations.forEach((crossKey, oldCount) -> {
            // check if this cross key still exists
            Integer newCount = newCrossRelations.get(crossKey);

            // get the data sources
            String source1 = crossKey.getSource1();
            String source2 = crossKey.getSource2();
            SzMatchType matchType = crossKey.getMatchType();
            String matchKey = crossKey.getMatchKey();
            String principle = crossKey.getPrinciple();

            // get the report code
            SzReportCode reportCode = (source1.equals(source2)) ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

            int recordDelta = 0;
            int entityDelta = 0;

            // check if no relationship still exists
            if (newCount == null) {
                recordDelta = -1 * oldCount;
                entityDelta = -1;
            } else {
                recordDelta = newCount - oldCount;
                entityDelta = (newCount == 0) ? -1 : 0;
            }

            SzReportStatistic statistic = STATISTIC_LOOKUP.get(matchType);

            reportUpdates.add(
                    builder(reportCode, statistic.principle(principle).matchKey(matchKey), source1, source2, entityId)
                            .records(recordDelta).entities(entityDelta).build());
        });

        // now iterate over the new cross relations that are completely new
        newCrossRelations.forEach((crossKey, newCount) -> {
            // check if this cross key previously existed -- and skip it if so
            if (oldCrossRelations.containsKey(crossKey)) {
                return;
            }

            // get the data sources
            String source1 = crossKey.getSource1();
            String source2 = crossKey.getSource2();
            SzMatchType matchType = crossKey.getMatchType();
            String principle = crossKey.getPrinciple();
            String matchKey = crossKey.getMatchKey();

            // get the report code
            SzReportCode reportCode = (source1.equals(source2)) ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

            SzReportStatistic statistic = STATISTIC_LOOKUP.get(matchType);

            reportUpdates.add(
                    builder(reportCode, statistic.principle(principle).matchKey(matchKey), source1, source2, entityId)
                            .records(newCount).entities(1).build());
        });
    }

    /**
     * Gets the {@link Map} of {@link CrossRelationKey} keys to {@link Integer}
     * values describing the record count for that key.
     *
     * @param sourceSummary  The source summary for the entity.
     * @param relatedSources The {@link Map} of {@link SzMatchType} keys to
     *                       {@link SortedSet} values describing the related sources
     *                       by match type.
     * @return The {@link Map} of {@link CrossRelationKey} keys to {@link Integer}
     *         values describing the record count for that key.
     */
    private static Map<CrossRelationKey, Integer> crossSourceRelations(Map<String, Integer> sourceSummary, Map<SourceRelationKey, SortedSet<String>> relatedSources) {
        Map<CrossRelationKey, Integer> result = new LinkedHashMap<>();
        sourceSummary.forEach((source, recordCount) -> {
            relatedSources.forEach(((relationKey, relatedSourceSet) -> {
                relatedSourceSet.forEach(relatedSource -> {
                    SzMatchType matchType = relationKey.getMatchType();
                    String matchKey = relationKey.getMatchKey();
                    String principle = relationKey.getPrinciple();
                    CrossRelationKey key = new CrossRelationKey(source, relatedSource, matchType, matchKey, principle);
                    result.put(key, recordCount);
                });
            }));
        });
        return result;
    }

    /**
     * Finds the changes to the {@link SzReportCode#ENTITY_SIZE_BREAKDOWN} report.
     *
     * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances.
     * @param oldEntity     The old version of the entity, which may be
     *                      <code>null</code>.
     * @param newEntity     The new version of the entity, which may be
     *                      <code>null</code>.
     * @param oldRecords    The {@link Set} of {@link SzRecord} instances describing
     *                      the old records.
     * @param newRecords    The {@link Set} of {@link SzRecord} instances describing
     *                      the new records.
     */
    private static void findEntitySizeChanges(List<SzReportUpdate> reportUpdates, SzResolvedEntity oldEntity, SzResolvedEntity newEntity, Map<SzRecordKey, SzRecord> oldRecords, Map<SzRecordKey, SzRecord> newRecords) {
        int oldSize = oldRecords.size();
        int newSize = newRecords.size();
        if (oldSize == newSize) {
            return;
        }

        long entityId = getEntityId(oldEntity, newEntity);

        // check if the entity previously existed
        if (oldEntity != null) {
            // it previously existed so remove its count from the entity size
            // breakdown for its previous size by decrementing entity count by 1
            reportUpdates.add(builder(ENTITY_SIZE_BREAKDOWN, oldSize, entityId).entities(-1).build());
        }

        // check if the entity still exists (may have been deleted)
        if (newEntity != null) {
            // since the entity exists, count it in the entity size breakdown for
            // its current size by incrementing the entity count by 1
            reportUpdates.add(builder(ENTITY_SIZE_BREAKDOWN, newSize, entityId).entities(1).build());
        }
    }

    /**
     * Finds the changes to the {@link SzReportCode#ENTITY_SIZE_BREAKDOWN} report.
     *
     * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances.
     * @param oldEntity     The old version of the entity, which may be
     *                      <code>null</code>.
     * @param newEntity     The new version of the entity, which may be
     *                      <code>null</code>.
     * @param oldRelations  The {@link Map} of {@link Long} entity ID's to
     *                      {@link SzRelatedEntity} values describing the old
     *                      relationships.
     * @param newRelations  The {@link Map} of {@link Long} entity ID's to
     *                      {@link SzRelatedEntity} values describing the new
     *                      relationships.
     */
    private static void findEntityRelationChanges(List<SzReportUpdate> reportUpdates, SzResolvedEntity oldEntity, SzResolvedEntity newEntity, Map<Long, SzRelatedEntity> oldRelations, Map<Long, SzRelatedEntity> newRelations) {
        int newCount = newRelations.size();
        int oldCount = oldRelations.size();

        long entityId = getEntityId(oldEntity, newEntity);

        // check if we have never seen this entity before
        if (oldEntity == null && newEntity != null) {
            // handle the case of a first-seen entity
            reportUpdates.add(builder(ENTITY_RELATION_BREAKDOWN, newCount, entityId).entities(1).build());

        } else if (newEntity == null && oldEntity != null) {
            // handle the case of a deleted entity
            // if the entity previously existed then remove it from its old
            // relationship breakdown count
            reportUpdates.add(builder(ENTITY_RELATION_BREAKDOWN, oldCount, entityId).entities(-1).build());

        } else if (oldEntity != null && newEntity != null && oldCount != newCount) {
            // handle the case of a different number of relationships
            // first decrement for the old count
            reportUpdates.add(builder(ENTITY_RELATION_BREAKDOWN, oldCount, entityId).entities(-1).build());

            // then increment for the new count
            reportUpdates.add(builder(ENTITY_RELATION_BREAKDOWN, newCount, entityId).entities(1).build());
        }
    }

    /**
     * Finds the {@linkplain SzReportCode#DATA_SOURCE_SUMMARY data source summary}
     * report updates that can be determined without knowing the relationship
     * updates.
     *
     * @param reportUpdates    The {@link List} of {@link SzReportUpdate} instances
     *                         to be populated.
     * @param oldEntity        The {@link SzEntity} describing the old state for the
     *                         entity.
     * @param newEntity        The {@link SzEntity} describing the new state for the
     *                         entity.
     * @param oldSourceSummary The {@link Map} of {@link String} data source code
     *                         keys to {@link Integer} values indicating the number
     *                         of records previously from that source in the entity.
     * @param newSourceSummary The {@link Map} of {@link String} data source code
     *                         keys to {@link Integer} values indicating the number
     *                         of records currently from that source in the entity.
     */
    private static void findSourceSummaryChanges(List<SzReportUpdate> reportUpdates, SzResolvedEntity oldEntity, SzResolvedEntity newEntity, Map<String, Integer> oldSourceSummary, Map<String, Integer> newSourceSummary) {
        long entityId = getEntityId(oldEntity, newEntity);

        // determine the old cross sources
        Map<CrossMatchKey, Integer> oldMatchCounts = countMatches(oldSourceSummary, oldEntity);

        // determine the new cross sources
        Map<CrossMatchKey, Integer> newMatchCounts = countMatches(newSourceSummary, newEntity);

        // determine the added cross-source pairs
        Map<CrossMatchKey, Integer> addedPairs = new TreeMap<>(newMatchCounts);
        addedPairs.keySet().removeAll(oldMatchCounts.keySet());

        // determine the removed cross-source pairs
        Map<CrossMatchKey, Integer> removedPairs = new TreeMap<>(oldMatchCounts);
        removedPairs.keySet().removeAll(newMatchCounts.keySet());

        // iterate over the removed pairs
        removedPairs.forEach((crossMatchKey, recordCount) -> {
            // get the sources
            String source1 = crossMatchKey.getSource1();
            String source2 = crossMatchKey.getSource2();

            // skip the cases where the sources differ
            if (!source1.equals(source2)) {
                return;
            }

            // get the match key and principle
            String matchKey = crossMatchKey.getMatchKey();
            String principle = crossMatchKey.getPrinciple();

            // handle entity count and unmatched count when no principle or match key
            if (matchKey == null && principle == null) {
                reportUpdates.add(
                        builder(DATA_SOURCE_SUMMARY, ENTITY_COUNT, source1, source2, entityId).entities(-1).build());

                if (recordCount == 1) {
                    reportUpdates.add(builder(DATA_SOURCE_SUMMARY, UNMATCHED_COUNT, source1, source2, entityId)
                            .entities(-1).records(-1).build());
                }
            }

            // check if we previously had a match (record count more than 1)
            if (recordCount > 1) {
                // handle the match count with the record count is greater than 1
                // create the statistic
                String statistic = MATCHED_COUNT.matchKey(matchKey).principle(principle).toString();

                reportUpdates.add(builder(DATA_SOURCE_SUMMARY, statistic, source1, source2, entityId).entities(-1)
                        .records(-1 * recordCount).build());
            }
        });

        addedPairs.forEach((crossMatchKey, recordCount) -> {
            // get the sources
            String source1 = crossMatchKey.getSource1();
            String source2 = crossMatchKey.getSource2();

            // skip the cases where the sources differ
            if (!source1.equals(source2)) {
                return;
            }

            // get the match key and principle
            String matchKey = crossMatchKey.getMatchKey();
            String principle = crossMatchKey.getPrinciple();

            // handle entity count and unmatched count when no principle or match key
            if (matchKey == null && principle == null) {
                reportUpdates.add(
                        builder(DATA_SOURCE_SUMMARY, ENTITY_COUNT, source1, source2, entityId).entities(1).build());

                if (recordCount == 1) {
                    reportUpdates.add(builder(DATA_SOURCE_SUMMARY, UNMATCHED_COUNT, source1, source2, entityId)
                            .entities(1).records(1).build());
                }
            }

            // check if we have a match (record count more than 1)
            if (recordCount > 1) {
                // handle the match count with the record count is greater than 1
                // create the statistic
                String statistic = MATCHED_COUNT.matchKey(matchKey).principle(principle).toString();

                reportUpdates.add(builder(DATA_SOURCE_SUMMARY, statistic, source1, source2, entityId).entities(1)
                        .records(recordCount).build());
            }
        });

        // now figure out the changed ones
        newMatchCounts.forEach((crossMatchKey, newCount) -> {
            // get the sources
            String source1 = crossMatchKey.getSource1();
            String source2 = crossMatchKey.getSource2();

            // skip the pairs where the data sources differ
            if (!source1.equals(source2)) {
                return;
            }

            // determine the difference -- skip if no difference
            Integer oldCount = oldMatchCounts.get(crossMatchKey);
            if (oldCount == null || newCount.equals(oldCount)) {
                return;
            }
            int diff = newCount - oldCount;

            // NOTE: no change to the entity count

            // get the match key and principle
            String matchKey = crossMatchKey.getMatchKey();
            String principle = crossMatchKey.getPrinciple();

            // handle entity count and unmatched count when no principle or match key
            if (matchKey == null && principle == null) {
                // check if it was previously counted as an "unmatched record", but now
                // it needs to be counted as a "matched record"
                if (oldCount == 1) {
                    // it was PREVIOUSLY an unmatched record -- need to decrement
                    reportUpdates.add(builder(DATA_SOURCE_SUMMARY, UNMATCHED_COUNT, source1, source2, entityId)
                            .entities(-1).records(-1).build());

                }

                // check if previously counted as a "matched record", but now it needs to
                // be counted as an "unmatched record"
                if (newCount == 1) {
                    reportUpdates.add(builder(DATA_SOURCE_SUMMARY, UNMATCHED_COUNT, source1, source2, entityId)
                            .entities(1).records(1).build());
                }
            }

            // format the statistic
            String statistic = MATCHED_COUNT.matchKey(crossMatchKey.getMatchKey())
                    .principle(crossMatchKey.getPrinciple()).toString();

            if (oldCount == 1 && newCount > 1) {
                // one more matched records now in the entity -- need to increment
                // the matched entity count and the record count by new count
                reportUpdates.add(builder(DATA_SOURCE_SUMMARY, statistic, source1, source2, entityId).entities(1)
                        .records(newCount).build());

            } else if (oldCount > 1 && newCount == 1) {
                // it is NOW an unmatched record
                // one less matched record entity -- decrement the old record count
                reportUpdates.add(builder(DATA_SOURCE_SUMMARY, statistic, source1, source2, entityId).entities(-1)
                        .records(-1 * oldCount).build());

            } else {
                // it was and is still counted in the MATCHED records, but size differs
                reportUpdates
                        .add(builder(DATA_SOURCE_SUMMARY, statistic, source1, source2, entityId).records(diff).build());
            }
        });
    }

    /**
     * Finds the {@linkplain SzReportCode#CROSS_SOURCE_SUMMARY cross source summary
     * match} report updates that can be determined without knowing the relationship
     * updates.
     *
     * @param reportUpdates    The {@link List} of {@link SzReportUpdate} instances
     *                         to be populated.
     * @param oldEntity        The {@link SzEntity} describing the old state for the
     *                         entity.
     * @param newEntity        The {@link SzEntity} describing the new state for the
     *                         entity.
     * @param oldSourceSummary The {@link Map} of {@link String} data source code
     *                         keys to {@link Integer} values indicating the number
     *                         of records previously from that source in the entity.
     * @param newSourceSummary The {@link Map} of {@link String} data source code
     *                         keys to {@link Integer} values indicating the number
     *                         of records currently from that source in the entity.
     */
    private static void findCrossMatchChanges(List<SzReportUpdate> reportUpdates, SzResolvedEntity oldEntity, SzResolvedEntity newEntity, Map<String, Integer> oldSourceSummary, Map<String, Integer> newSourceSummary) {
        long entityId = getEntityId(oldEntity, newEntity);

        // determine the old cross sources
        Map<CrossMatchKey, Integer> oldMatchCounts = countMatches(oldSourceSummary, oldEntity);

        // determine the new cross sources
        Map<CrossMatchKey, Integer> newMatchCounts = countMatches(newSourceSummary, newEntity);

        // determine the added cross-source pairs
        Map<CrossMatchKey, Integer> addedPairs = new TreeMap<>(newMatchCounts);
        addedPairs.keySet().removeAll(oldMatchCounts.keySet());

        // determine the removed cross-source pairs
        Map<CrossMatchKey, Integer> removedPairs = new TreeMap<>(oldMatchCounts);
        removedPairs.keySet().removeAll(newMatchCounts.keySet());

        removedPairs.forEach((crossMatchKey, recordCount) -> {
            // get the sources
            String source1 = crossMatchKey.getSource1();
            String source2 = crossMatchKey.getSource2();

            // skip the cases where the sources are the same
            if (source1.equals(source2)) {
                return;
            }

            // create the statistic
            String statistic = MATCHED_COUNT.matchKey(crossMatchKey.getMatchKey())
                    .principle(crossMatchKey.getPrinciple()).toString();

            reportUpdates.add(builder(CROSS_SOURCE_SUMMARY, statistic, source1, source2, entityId).entities(-1)
                    .records(-1 * recordCount).build());
        });

        // iterated over the added pairs of data sources
        addedPairs.forEach((crossMatchKey, recordCount) -> {
            // get the sources
            String source1 = crossMatchKey.getSource1();
            String source2 = crossMatchKey.getSource2();

            if (source1.equals(source2)) {
                return;
            }

            String statistic = MATCHED_COUNT.matchKey(crossMatchKey.getMatchKey())
                    .principle(crossMatchKey.getPrinciple()).toString();

            reportUpdates.add(builder(CROSS_SOURCE_SUMMARY, statistic, source1, source2, entityId).entities(1)
                    .records(recordCount).build());
        });

        // iterate over the pairs that exist in both maps
        newMatchCounts.forEach((crossMatchKey, newCount) -> {
            // get the sources
            String source1 = crossMatchKey.getSource1();
            String source2 = crossMatchKey.getSource2();

            // if the sources are the same then skip this one
            if (source1.equals(source2)) {
                return;
            }

            // determine the difference -- skip if no difference
            Integer oldCount = oldMatchCounts.get(crossMatchKey);
            if (oldCount == null || newCount.equals(oldCount)) {
                return;
            }
            int diff = newCount - oldCount;

            // format the statistic
            String statistic = MATCHED_COUNT.matchKey(crossMatchKey.getMatchKey())
                    .principle(crossMatchKey.getPrinciple()).toString();

            // log the report updates
            reportUpdates
                    .add(builder(CROSS_SOURCE_SUMMARY, statistic, source1, source2, entityId).records(diff).build());
        });
    }

    /**
     * Counts the number of matched records of each data source to each other data
     * source and returns a {@link Map} of {@link CrossMatchKey} keys to
     * {@link Integer} values for the count.
     *
     * @param summary The {@link Map} of {@link String} data source code keys to
     *                {@link Integer} values describing the record counts per data
     *                source for the entity.
     * 
     * @param entity  The {@link SzEntity} for which to count the matches.
     *
     * @return The {@link Map} of {@link List} values containing exactly two
     *         {@link String} data source codes (the "from" data source code,
     *         followed by the "to" data source code) and {@link Integer} values
     *         describing the number of records in the first data source.
     */
    private static Map<CrossMatchKey, Integer> countMatches(Map<String, Integer> summary, SzEntity entity) {
        // create the result map
        Map<CrossMatchKey, Integer> matchCounts = new TreeMap<>();

        // check if the entity is null
        if (entity == null) {
            return matchCounts;
        }

        // get the match key pairs
        SortedSet<MatchPairKey> matchKeyPairs = getMatchPairs(entity);

        // determine the old cross sources
        summary.forEach((source1, recordCount1) -> {
            summary.forEach((source2, recordCount2) -> {
                matchKeyPairs.forEach(matchPairKey -> {
                    String matchKey = matchPairKey.getMatchKey();
                    String principle = matchPairKey.getPrinciple();

                    CrossMatchKey crossMatchKey = new CrossMatchKey(source1, source2, matchKey, principle);

                    matchCounts.put(crossMatchKey, recordCount1);
                });
            });
        });

        // return the result
        return matchCounts;
    }

    /**
     * Gets the distinct set of {@link MatchPairKey} instances describing the match
     * keys, principles and pairs of match keys and principles for the entity
     * described by the specified {@link SzEntity}.
     * 
     * @param entity The {@link SzEntity} from which to extract the distinct
     *               {@link SortedSet} of {@link MatchPairKey} instances.
     * 
     * @return The {@link SortedSet} of {@link MatchPairKey} instances for the
     *         records in the specified {@link SzEntity}.
     */
    private static SortedSet<MatchPairKey> getMatchPairs(SzEntity entity) {
        // create the result map
        SortedSet<MatchPairKey> result = new TreeSet<>();

        // check if the entity is null
        if (entity == null) {
            return result;
        }

        // iterate over the records
        entity.getRecords().values().forEach(record -> {
            String matchKey = record.getMatchKey();
            String principle = record.getPrinciple();
            result.add(new MatchPairKey(matchKey, null));
            result.add(new MatchPairKey(null, principle));
            result.add(new MatchPairKey(matchKey, principle));
        });
        return result;
    }

    /**
     * Cross the data source summaries of an entity and a related entity into a
     * {@link Map} of data source code pairs. When the data sources are the same,
     * then the record counts are added in the resulted {@link Map}, and when they
     * differ the count is taken from the first data source code in the pair.
     *
     * @param summary1 The first {@link Map} of {@link String} data source code keys
     *                 to {@link Integer} values describing the record counts per
     *                 data source.
     * @param summary2 The second {@link Map} of {@link String} data source code
     *                 keys to {@link Integer} values describing the record counts
     *                 per data source.
     *
     * @return The {@link Map} of {@link List} values containing exactly two
     *         {@link String} data source codes and {@link RelationshipCounts}
     *         values describing the ounts for the relationships.
     */
    private static Map<List<String>, RelationshipCounts> countRelationships(Map<String, Integer> summary1, Map<String, Integer> summary2) {
        Map<List<String>, RelationshipCounts> result = new LinkedHashMap<>();

        // get the forward counts
        summary1.forEach((source1, count1) -> {
            summary2.forEach((source2, count2) -> {
                // create the key
                List<String> key = List.of(source1, source2);

                // get the counts if any (create if not)
                RelationshipCounts counts = result.get(key);
                if (counts == null) {
                    counts = new RelationshipCounts(source1, source2);
                    result.put(key, counts);
                }

                counts.setRecordCount(count1);
            });
        });

        // return the result
        return result;
    }

    /**
     * Determines the delta counts between the old and the new maps.
     *
     * @param oldCrossSource The {@link Map} of cross-source pair keys to the old
     *                       counts as {@Integer} values.
     * @param newCrossSource The {@link Map} of cross-source pair keys to the new
     *                       counts as {@link Integer} values.
     * @return The {@link Map} of cross-source pair keys to {@link Integer} delta
     *         values.
     */
    private static Map<List<String>, Integer> diffCrossSourceSummaries(Map<List<String>, Integer> oldCrossSource, Map<List<String>, Integer> newCrossSource) {
        Map<List<String>, Integer> result = new LinkedHashMap<>();

        oldCrossSource.forEach((sourcePair, oldCount) -> {
            // get the new count
            Integer newCount = newCrossSource.get(sourcePair);

            // if no new count, then default to zero
            if (newCount == null) {
                newCount = 0;
            }

            // put the difference in the map
            result.put(sourcePair, newCount - oldCount);
        });
        newCrossSource.forEach((sourcePair, newCount) -> {
            // get the old count
            Integer oldCount = oldCrossSource.get(sourcePair);

            // check if already handled
            if (oldCount != null) {
                return;
            }

            // put the difference in the map
            result.put(sourcePair, newCount);
        });

        return result;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link SortedSet} of {@link ResourceKey}
     * instances identifying the resources that need to be locked during database
     * updated.
     *
     * @return The <b>unmodifiable</b> {@link SortedSet} of {@link ResourceKey}
     *         instances identifying the resources that need to be locked during
     *         database updated.
     */
    public SortedSet<ResourceKey> getResourceKeys() {
        return this.resourceKeys;
    }

    /**
     * Creates the variant combinations of the specified {@link Statistic} formatted
     * by itself, with only the principle, only the match key and with both the
     * principle and match key.
     * 
     * @param statistic The {@link SzReportStatistic} to use.
     * @param principle The principle to use for the variant combinations.
     * @param matchKey  The match key to use for the variant combinations.
     * 
     * @return The {@link Set} of variants.
     */
    private static Set<String> statisticVariants(SzReportStatistic statistic, String principle, String matchKey) {
        // normalize the parameters
        if (principle != null) {
            principle = principle.trim();
        }
        if (matchKey != null) {
            matchKey = matchKey.trim();
        }
        if (principle.length() == 0) {
            principle = null;
        }
        if (matchKey.length() == 0) {
            matchKey = null;
        }

        Set<String> result = new TreeSet<>();
        result.add(statistic.toString());

        if (principle != null) {
            result.add(statistic.principle(principle).format());
        }
        if (matchKey != null) {
            result.add(statistic.matchKey(matchKey).format());
        }
        if (principle != null && matchKey != null) {
            result.add(statistic.principle(principle).matchKey(matchKey).format());
        }
        return result;
    }

    /**
     * Creates the {@link Set} of variant statistic combinations that need to be
     * decremented based on the change in relationship match type, principle, and/or
     * match key.
     * 
     * @param oldMatchType The old match type for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchType The new match type for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldPrinciple The old principle for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newPrinciple The new principle for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldMatchKey  The old match key for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchKey  The new match key for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * 
     * @return The {@link Set} of {@link String} statistics that need to be
     *         decremented.
     */
    private static Set<String> decrementStatVariants(SzMatchType oldMatchType, SzMatchType newMatchType, String oldPrinciple, String newPrinciple, String oldMatchKey, String newMatchKey) {
        checkStatVariantParameters(oldMatchType, newMatchType, oldPrinciple, newPrinciple, oldMatchKey, newMatchKey);

        Set<String> result = new TreeSet<>();

        // check if nothing to decrement
        if (oldMatchType == null || ((oldMatchType == newMatchType) && (Objects.equals(oldPrinciple, newPrinciple))
                && (Objects.equals(oldMatchKey, newMatchKey)))) {
            return result;
        }

        // get the old statistic
        SzReportStatistic statistic = STATISTIC_LOOKUP.get(oldMatchType);

        // check if decrementing based on match type -- then everything goes
        if (oldMatchType != newMatchType) {
            return statisticVariants(statistic, oldPrinciple, oldMatchKey);
        }

        // we have the same match type so check the principle
        if (!Objects.equals(oldPrinciple, newPrinciple)) {
            result.add(statistic.principle(oldPrinciple).format());
            result.add(statistic.principle(oldPrinciple).matchKey(oldMatchKey).format());
        }

        // we have the same match type so check the match key
        if (!Objects.equals(oldMatchKey, newMatchKey)) {
            result.add(statistic.matchKey(oldMatchKey).format());
            result.add(statistic.matchKey(oldMatchKey).principle(oldPrinciple).format());
        }

        // return the result
        return result;
    }

    /**
     * Creates the {@link Set} of variant statistic combinations that need to be
     * incremented based on the change in relationship match type, principle, and/or
     * match key.
     * 
     * @param oldMatchType The old match type for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchType The new match type for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldPrinciple The old principle for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newPrinciple The new principle for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldMatchKey  The old match key for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchKey  The new match key for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * 
     * @return The {@link Set} of {@link String} statistics that need to be
     *         decremented.
     */
    private static Set<String> incrementStatVariants(SzMatchType oldMatchType, SzMatchType newMatchType, String oldPrinciple, String newPrinciple, String oldMatchKey, String newMatchKey) {
        checkStatVariantParameters(oldMatchType, newMatchType, oldPrinciple, newPrinciple, oldMatchKey, newMatchKey);

        Set<String> result = new TreeSet<>();

        // check if nothing to increment
        if (newMatchType == null || ((oldMatchType == newMatchType) && (Objects.equals(oldPrinciple, newPrinciple))
                && (Objects.equals(oldMatchKey, newMatchKey)))) {
            return result;
        }

        // get the new statistic
        SzReportStatistic statistic = STATISTIC_LOOKUP.get(newMatchType);

        // check if incrementing based on match type -- then everything goes
        if (oldMatchType != newMatchType) {
            return statisticVariants(statistic, newPrinciple, newMatchKey);
        }

        // we have the same match type so check the principle
        if (!Objects.equals(oldPrinciple, newPrinciple)) {
            result.add(statistic.principle(newPrinciple).format());
            result.add(statistic.principle(newPrinciple).matchKey(newMatchKey).format());
        }

        // we have the same match type so check the match key
        if (!Objects.equals(oldMatchKey, newMatchKey)) {
            result.add(statistic.matchKey(newMatchKey).format());
            result.add(statistic.matchKey(newMatchKey).principle(newPrinciple).format());
        }

        // return the result
        return result;
    }

    /**
     * Creates the {@link Set} of variant statistic combinations that require deltas
     * to be handled for data source combinations when the match type, principle
     * and/or match key are the same.
     * 
     * @param oldMatchType The old match type for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchType The new match type for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldPrinciple The old principle for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newPrinciple The new principle for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldMatchKey  The old match key for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchKey  The new match key for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * 
     * @return The {@link Set} of {@link String} statistics that need to be
     *         decremented.
     */
    private static Set<String> deltaStatVariants(SzMatchType oldMatchType, SzMatchType newMatchType, String oldPrinciple, String newPrinciple, String oldMatchKey, String newMatchKey) {
        checkStatVariantParameters(oldMatchType, newMatchType, oldPrinciple, newPrinciple, oldMatchKey, newMatchKey);

        Set<String> result = new TreeSet<>();

        // check if nothing for which to compute deltas
        if ((newMatchType == null && oldMatchType == null) || (newMatchType != oldMatchType)) {
            return result;
        }

        // get the old statistic
        SzReportStatistic statistic = STATISTIC_LOOKUP.get(newMatchType);

        // we have the same match type so add the base statistic
        result.add(statistic.toString());

        // check if the principle is also the same
        if (Objects.equals(oldPrinciple, newPrinciple)) {
            result.add(statistic.principle(newPrinciple).format());
        }

        // check if the match key is also the same
        if (Objects.equals(oldMatchKey, newMatchKey)) {
            result.add(statistic.matchKey(newMatchKey).format());
        }

        // check if both the principle and match key are the same
        if (Objects.equals(oldPrinciple, newPrinciple) && Objects.equals(oldMatchKey, newMatchKey)) {
            result.add(statistic.matchKey(newMatchKey).principle(newPrinciple).format());
        }

        // return the result
        return result;
    }

    /**
     * Checks for sanity in the match type, principle and match key parameters for
     * when computing the stat variants.
     * 
     * @param oldMatchType The old match type for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchType The new match type for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldPrinciple The old principle for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newPrinciple The new principle for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * @param oldMatchKey  The old match key for the relationship, or
     *                     <code>null</code> if this is a new relationship.
     * @param newMatchKey  The new match key for the relationship, or
     *                     <code>null</code> if the relationship no longer exists.
     * 
     * @throws IllegalArgumentException If a null value is encountered with the
     *                                  corresponding parameters NOT also being
     *                                  null.
     */
    private static void checkStatVariantParameters(SzMatchType oldMatchType, SzMatchType newMatchType, String oldPrinciple, String newPrinciple, String oldMatchKey, String newMatchKey) throws IllegalArgumentException {
        // check the parameters for the old relationship
        if ((oldMatchType == null || oldPrinciple == null || oldMatchKey == null)
                && (oldMatchType != null || oldPrinciple != null || oldMatchKey != null)) {
            throw new IllegalArgumentException("If the relationship did not previously exist then the old match type, "
                    + "principle AND match key must all be null.  If it did previously exist, "
                    + "then ALL must NOT be null.  mathType=[ " + oldMatchType + " ], principle=[ " + oldPrinciple
                    + " ], matchKey=[ " + oldMatchKey + " ]");
        }

        // check the parameters for the new relationship
        if ((newMatchType == null || newPrinciple == null || newMatchKey == null)
                && (newMatchType != null || newPrinciple != null || newMatchKey != null)) {
            throw new IllegalArgumentException(
                    "If the relationship no longer exists then the new match type, principle "
                    + "AND match key must all be null.  If it does exist, then ALL must NOT "
                    + "be null.  mathType=[ " + oldMatchType + " ], principle=[ " + oldPrinciple
                    + " ], matchKey=[ " + oldMatchKey + " ]");
        }
    }
}
