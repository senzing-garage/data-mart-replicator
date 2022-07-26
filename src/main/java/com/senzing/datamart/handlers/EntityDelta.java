package com.senzing.datamart.handlers;

import com.senzing.datamart.model.*;

import java.util.*;

import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.model.SzReportStatistic.*;
import static com.senzing.datamart.model.SzReportStatistic.DUPLICATE_COUNT;
import static com.senzing.datamart.model.SzReportUpdate.builder;
import static com.senzing.datamart.model.SzMatchType.*;

/**
 * Determines the delta between the old state of an entity and the new state
 * of the same entity (i.e.: the one with the same entity ID).  In some cases
 * the old state or new state may be that the entity does not exist, however,
 * the both states should not be non-existent or else no difference exists.
 */
public class EntityDelta {
  /**
   * The lookup {@link Map} of {@link SzMatchType} keys to
   * {@link SzReportStatistic} values.
   */
  private static final Map<SzMatchType, SzReportStatistic> STATISTIC_LOOKUP
      = Map.of(AMBIGUOUS_MATCH, AMBIGUOUS_MATCH_COUNT,
               POSSIBLE_MATCH, POSSIBLE_MATCH_COUNT,
               DISCLOSED_RELATION, DISCLOSED_RELATION_COUNT,
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
   * The difference between old and new for the number of records that the
   * entity is comprised of.
   */
  private int recordDelta;

  /**
   * The difference between old and new for the number of relationships the
   * entity has.
   */
  private int relationDelta;

  /**
   * The record-count deltas by data source between the old and the new
   * data source summaries.
   */
  private Map<String, Integer> dataSourceDeltas;

  /**
   * The records that were added to the entity, which may be an empty list.
   */
  private Set<SzRecord> addedRecords;

  /**
   * The {@link Set} of {@link SzRecord} instances that have been created.
   */
  private Set<SzRecord> createdRecords;

  /**
   * The records that were removed from the entity, which may be an empty list.
   */
  private Set<SzRecord> removedRecords;

  /**
   * The {@link Set} of {@link SzRecord} instances that have been deleted.
   */
  private Set<SzRecord> deletedRecords;

  /**
   * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
   * values describing the related entities that related to the entity, but
   * previously were not.
   */
  private Map<Long, SzRelatedEntity> addedRelations;

  /**
   * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
   * values describing the previously related entities that are no longer
   * related to the entity.
   */
  private Map<Long, SzRelatedEntity> removedRelations;

  /**
   * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
   * values describing the related entities for which a change in the
   * relationship occurred.
   */
  private Map<Long, SzRelatedEntity> changedRelations;

  /**
   * The {@link List} of {@link SzReportUpdate} instances detected from the
   * differences.
   */
  private List<SzReportUpdate> reportUpdates;

  /**
   * Default constructor.
   */
  public EntityDelta(SzResolvedEntity oldEntity, SzResolvedEntity newEntity) {
    if (oldEntity != null && newEntity != null
        && oldEntity.getEntityId() != newEntity.getEntityId())
    {
     throw new IllegalArgumentException(
         "The entities for which the differences are being discovered must "
         + "describe the same entity at different points in time, rather than "
         + "two different entities.  oldEntityId=[ " + oldEntity.getEntityId()
         + " ], newEntityId=[ " + newEntity.getEntityId() + " ]");
    }

    // set the entity fields
    this.oldEntity = oldEntity;
    this.newEntity = newEntity;

    // get the old and new data sources
    Map<String, Integer> oldSourceSummary = getSourceSummary(oldEntity);
    Map<String, Integer> newSourceSummary = getSourceSummary(newEntity);

    this.dataSourceDeltas = new LinkedHashMap<>();
    oldSourceSummary.forEach((source, oldCount) -> {
      Integer newCount = newSourceSummary.get(source);
      if (newCount == null) return;
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

    // determine if the data sources changed
    boolean dataSourcesChanged = (! oldDataSources.equals(newDataSources));

    // determine the record count delta
    Set<SzRecord> oldRecords = getRecords(oldEntity);
    Set<SzRecord> newRecords = getRecords(newEntity);

    this.recordDelta = newRecords.size() - oldRecords.size();

    // determine the added records
    this.addedRecords = findAddedRecords(oldRecords, newRecords);

    // determine the removed records
    this.removedRecords = findRemovedRecords(oldRecords, newRecords);

    // determine the relationship count delta
    Map<Long, SzRelatedEntity> oldRelations = getRelatedEntities(oldEntity);
    Map<Long, SzRelatedEntity> newRelations = getRelatedEntities(newEntity);

    this.relationDelta = newRelations.size() - oldRelations.size();

    // determine the added relationships
    this.addedRelations = findAddedRelations(oldRelations, newRelations);

    // determine the removed relationships
    this.removedRelations = findRemovedRelations(oldRelations, newRelations);

    // determine the changed relationships
    this.changedRelations = findChangedRelations(oldDataSources,
                                                 newDataSources,
                                                 oldRelations,
                                                 newRelations);

    // determine the ECB (Entity Count Breakdown) changes
    this.reportUpdates = new LinkedList<>();

    // check the entity size breakdown
    findEntitySizeChanges(
        this.reportUpdates, oldEntity, newEntity, oldRecords, newRecords);

    // check the entity relation breakdown
    findEntityRelationChanges(
        this.reportUpdates, oldEntity, newEntity, oldRelations, newRelations);

    // check the data source summary
    findSourceSummaryChanges(this.reportUpdates,
                             oldEntity,
                             newEntity,
                             oldSourceSummary,
                             newSourceSummary);

    // check the cross source summary
    findCrossSummaryChanges(this.reportUpdates,
                             oldEntity,
                             newEntity,
                             oldSourceSummary,
                             newSourceSummary);

    // initialize the remaining members
    this.createdRecords = new LinkedHashSet<>();
    this.deletedRecords = new LinkedHashSet<>();
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
   * Gets the {@link Set} of {@link SzRecord} instances describing the records
   * for the old version of the entity.  If the entity did not previously exist
   * (i.e.: the old entity is <code>null</code>) then this returns an empty
   * {@link Set}.
   *
   * @return The {@link Set} of {@link SzRecord} instances describing the
   *         records for the old version of the entity, or <code>null</code> if
   *         the entity did not previously exist.
   */
  public Set<SzRecord> getOldRecords() {
    SzResolvedEntity entity = this.getOldEntity();
    return (entity == null) ? Collections.emptySet() : entity.getRecords();
  }

  /**
   * Gets the {@link Set} of {@link SzRecord} instances describing the records
   * for the new version of the entity.  If the entity was deleted (i.e.: the
   * new entity is <code>null</code>) then this returns an empty {@link Set}.
   *
   * @return The {@link Set} of {@link SzRecord} instances describing the
   *         records for the new version of the entity, or <code>null</code> if
   *         the entity was deleted.
   */
  public Set<SzRecord> getNewRecords() {
    SzResolvedEntity entity = this.getNewEntity();
    return (entity == null) ? Collections.emptySet() : entity.getRecords();
  }

  /**
   * Gets the {@link Map} of {@link String} data source code keys to {@link
   * Integer} describing the record counts per data source for the old version
   * of the entity.  If the entity did not previously exist (i.e.: the old
   * entity is <code>null</code>) then this returns an empty {@link Map}.
   *
   * @return The {@link Map} of {@link String} data source code keys to {@link
   *         Integer} describing the record counts per data source for the old
   *         version of the entity.
   */
  public Map<String, Integer> getOldSourceSummary() {
    return getSourceSummary(this.getOldEntity());
  }

  /**
   * Gets the {@link Map} of {@link String} data source code keys to {@link
   * Integer} describing the record counts per data source for the new version
   * of the entity.  If the entity was deleted (i.e.: the new entity is
   * <code>null</code>) then this returns an empty {@link Map}.
   *
   * @return The {@link Map} of {@link String} data source code keys to {@link
   *         Integer} describing the record counts per data source for the new
   *         version of the entity.
   */
  public Map<String, Integer> getNewSourceSummary() {
    return getSourceSummary(this.getNewEntity());
  }

  /**
   * Gets the {@link Map} of {@link Long} entity ID keys to {@link
   * SzRelatedEntity} instances describing the entities previously related to
   * this entity.  If the entity previously did not exist (i.e.: the old entity
   * is <code>null</code>) then this returns an empty {@link Map}.
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} instances describing the entities previously
   *         related to this entity.
   */
  public Map<Long, SzRelatedEntity> getOldRelatedEntities() {
    return getRelatedEntities(this.getOldEntity());
  }

  /**
   * Gets the {@link Map} of {@link Long} entity ID keys to {@link
   * SzRelatedEntity} instances describing the currently related entities to
   * this entity.  If the entity no longer exists (i.e.: the new entity
   * is <code>null</code>) then this returns an empty {@link Map}.
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} instances describing the entities currently
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
   * Gets the {@link Map} of {@link String} data source code keys to {@link
   * Integer} values describing the deltas in the record counts by data source
   * between the old and the new version of the entity.
   *
   * @return The {@link Map} of {@link String} data source code keys to {@link
   *         Integer} values describing the deltas in the record counts by data
   *         source between the old and the new version of the entity.
   */
  public Map<String, Integer> getDataSourceDeltas() {
    return this.dataSourceDeltas;
  }

  /**
   * Gets the {@link Set} of {@link SzRecord} instances describing the
   * records that were added at the entity.
   *
   * @return The {@link Set} of {@link SzRecord} instances describing the
   *         records that were added ot the entity.
   */
  public Set<SzRecord> getAddedRecords() {
    return Collections.unmodifiableSet(this.addedRecords);
  }

  /**
   * Indicates that the specified record that was added to the entity had to be
   * created new in the data mart.
   *
   * @param record The {@linkS SzRecord} describing the record that was newly
   *               created.
   * @throws NullPointerException If the specified {@link SzRecord} is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified {@link SzRecord} is not
   *                                  one of the {@linkplain #getAddedRecords()
   *                                  added records} or has already been marked
   *                                  as created.
   */
  public void createdRecord(SzRecord record) {
    Objects.requireNonNull(record, "The record cannot be null");
    if (!this.getAddedRecords().contains(record)) {
      throw new IllegalArgumentException(
          "The specified record is not one of the added records.  record=[ "
              + record + " ], allowed=[ " + this.getAddedRecords() + " ]");
    }
    if (this.createdRecords.contains(record)) {
      throw new IllegalArgumentException(
          "The specified record has already been marked as created.  record=[ "
          + record + " ]");
    }
    this.createdRecords.add(record);

    String  source    = record.getDataSource();
    long    entityId  = this.getEntityId();
    this.reportUpdates.add(
        builder(
            DATA_SOURCE_SUMMARY, RECORD_COUNT, source, source, entityId)
            .records(1).build());
  }

  /**
   * Gets the {@link Set} of {@link SzRecord} instances describing the
   * records that were removed from the entity.
   *
   * @return The {@link Set} of {@link SzRecord} instances describing the
   *         records that were removed from the entity.
   */
  public Set<SzRecord> getRemovedRecords() {
    return Collections.unmodifiableSet(this.removedRecords);
  }

  /**
   * Indicates that the specified record that was added to the entity had to be
   * created new in the data mart.
   *
   * @param record The {@linkS SzRecord} describing the record that was newly
   *               created.
   * @throws NullPointerException If the specified {@link SzRecord} is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified {@link SzRecord} is not
   *                                  one of the {@linkplain #getAddedRecords()
   *                                  added records} or has already been marked
   *                                  as created.
   */
  public void orphanedRecord(SzRecord record) {
    Objects.requireNonNull(record, "The record cannot be null");
    if (!this.getRemovedRecords().contains(record)) {
      throw new IllegalArgumentException(
          "The specified record is not one of the removed records.  record=[ "
              + record + " ], allowed=[ " + this.getRemovedRecords() + " ]");
    }
    if (this.deletedRecords.contains(record)) {
      throw new IllegalArgumentException(
          "The specified record has already been marked as deleted.  record=[ "
              + record + " ]");
    }

    String  source    = record.getDataSource();
    long    entityId  = this.getEntityId();

    this.deletedRecords.add(record);

    reportUpdates.add(
        builder(
            DATA_SOURCE_SUMMARY, RECORD_COUNT, source, source, entityId)
            .records(-1).build());
  }

  /**
   * Gets the {@link Map} of {@link Long} entity ID keys to {@link
   * SzRelatedEntity} values describing the related entities that are now
   * related to the entity, but previously were not related.
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the related entities that are
   *         now related to the entity, but previously were not related.
   */
  public Map<Long, SzRelatedEntity> getAddedRelations() {
    return Collections.unmodifiableMap(this.addedRelations);
  }

  /**
   * Gets the {@link Map} of {@link Long} entity ID keys to {@link
   * SzRelatedEntity} values describing the related entities for which the
   * relationship has changed in some way (e.g.: new data sources for this
   * entity or the other).
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the related entities that are
   *         no longer related to the entity, but previously were related.
   */
  public Map<Long, SzRelatedEntity> getChangedRelations() {
    return Collections.unmodifiableMap(this.changedRelations);
  }

  /**
   * Marks the relationship to the specified related entity as having been
   * stored in the database.  The relationship may have been created or updated.
   *
   * @param entityId The entity ID of the first entity in the relationship.
   * @param relatedId The entity ID of the second entity in the relationship.
   * @param oldMatchType The previous {@link SzMatchType} stored in the
   *                     data mart database, or <code>null</code> if not
   *                     previously stored.
   * @param oldSourceSummary The {@link Map} of {@link String} data source keys
   *                         to {@link Integer} values providing the data
   *                         source record summary of the entity identified by
   *                         <code>entityId</code> previously associated with
   *                         the relationship, or <code>null</code> (or empty)
   *                         if the relationship was newly created.
   * @param oldRelatedSummary The {@link Map} of {@link String} data source
   *                          keys to {@link Integer} values providing the data
   *                          source record summary of the entity identified by
   *                          <code>relatedId</code> previously associated with
   *                          the relationship, or <code>null</code> (or empty)
   *                          if the relationship was newly created.
   * @throws IllegalArgumentException If the specified entity ID's are not
   *                                  different or neither matches the entity ID
   *                                  for this instance.
   */
  public void storedRelationship(long                 entityId,
                                 long                 relatedId,
                                 SzMatchType          oldMatchType,
                                 Map<String, Integer> oldSourceSummary,
                                 Map<String, Integer> oldRelatedSummary)
  {
    // get our entity ID
    long myEntityId   = this.getEntityId();

    // normalize possibly-null arguments
    if (oldSourceSummary == null) oldSourceSummary = Collections.emptyMap();
    if (oldRelatedSummary == null) oldRelatedSummary = Collections.emptyMap();

    // check that our entity ID is one of the specified entity ID's
    if (entityId != myEntityId && relatedId != myEntityId) {
      throw new IllegalArgumentException(
          "Neither of the specified entity ID's match the entity ID for this "
              + "instance.  entityId=[ " + entityId + " ], relatedId=[ "
              + relatedId + " ], expectedId=[ " + myEntityId + " ]");
    }
    // check both entity ID's are not equal
    if (entityId == relatedId) {
      throw new IllegalArgumentException(
          "Both the entity ID and the related entity ID cannot be the same: "
          + entityId);
    }

    // normalize everything with respect to this entity's perspective
    if (myEntityId == relatedId) {
      relatedId                 = entityId;
      entityId                  = myEntityId;
      Map<String, Integer> temp = oldSourceSummary;
      oldSourceSummary          = oldRelatedSummary;
      oldRelatedSummary         = temp;
    }

    // now check that the related entity ID is related to the entity
    if (!this.getAddedRelations().containsKey(relatedId)
        && !this.getChangedRelations().containsKey(relatedId))
    {
      throw new IllegalArgumentException(
          "Cannot store the relationship to an entity for which the "
          + "relationship was not added or changed.  relatedId=[ " + relatedId
          + " ], added=[ " + this.getAddedRelations().keySet()
              + " ], changed=[ " + this.getChangedRelations().keySet() + " ]");
    }

    // determine the previous cross-source pairs
    Map<List<String>, Integer> oldCrossSources = crossSourceSummaries(
        oldSourceSummary, oldRelatedSummary);

    // get the related entities
    Map<Long, SzRelatedEntity> relatedEntityMap = this.getNewRelatedEntities();
    SzRelatedEntity related = relatedEntityMap.get(relatedId);

    // get the data source summary
    Map<String, Integer> newSourceSummary   = this.getNewSourceSummary();
    Map<String, Integer> newRelatedSummary  = related.getSourceSummary();

    // determine the new cross-source pairs
    Map<List<String>, Integer> newCrossSources = crossSourceSummaries(
        newSourceSummary, newRelatedSummary);

    // get the current match type
    SzMatchType matchType = related.getMatchType();

    // determine the lesser and greater entity ID
    long id1  = (entityId < relatedId) ? entityId : relatedId;
    long id2  = (entityId < relatedId) ? relatedId : entityId;

    // check if we need to decrement counts for an old match type
    if (matchType != oldMatchType && oldMatchType != null) {
      // get the statistic
      SzReportStatistic statistic = STATISTIC_LOOKUP.get(oldMatchType);

      // handle decrementing the counts accordingly
      oldCrossSources.forEach((sourcePair, count) -> {
        String source1 = sourcePair.get(0);
        String source2 = sourcePair.get(1);

        SzReportCode reportCode = (source1.equals(source2))
            ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

        // delete all the old counts for this relationship
        reportUpdates.add(
            builder(
                reportCode, statistic, source1, source2, id1, id2)
                .relations(-1).recordRelations(-1 * count).build());
      });
    }

    // check if need to increment counts for the new match type
    if (matchType != oldMatchType && matchType != null) {
      // get the statistic
      SzReportStatistic statistic = STATISTIC_LOOKUP.get(matchType);

      // handle decrementing the counts accordingly
      newCrossSources.forEach((sourcePair, count) -> {
        String source1 = sourcePair.get(0);
        String source2 = sourcePair.get(1);

        SzReportCode reportCode = (source1.equals(source2))
            ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

        // delete all the old counts for this relationship
        reportUpdates.add(
            builder(
                reportCode, statistic, source1, source2, id1, id2)
                .relations(1).recordRelations(count).build());
      });
    }

    // check if the match type is the same and compute the deltas
    if (matchType == oldMatchType && matchType != null && oldMatchType != null)
    {
      Map<List<String>, Integer> crossSourceDeltas = diffCrossSourceSummaries(
          oldCrossSources, newCrossSources);

      // get the statistic
      SzReportStatistic statistic = STATISTIC_LOOKUP.get(matchType);

      // handle updating the counts accordingly
      crossSourceDeltas.forEach((sourcePair, delta) -> {
        String source1 = sourcePair.get(0);
        String source2 = sourcePair.get(1);

        SzReportCode reportCode = (source1.equals(source2))
            ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

        // delete all the old counts for this relationship
        reportUpdates.add(
            builder(
                reportCode, statistic, source1, source2, id1, id2)
                .recordRelations(delta).build());
      });
    }
  }

  /**
   * Gets the {@link Map} of {@link Long} entity ID keys to {@link
   * SzRelatedEntity} values describing the related entities that are no
   * longer related to the entity, but previously were related.
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the related entities that are
   *         no longer related to the entity, but previously were related.
   */
  public Map<Long, SzRelatedEntity> getRemovedRelations() {
    return Collections.unmodifiableMap(this.removedRelations);
  }

  /**
   * Marks the relationship to the specified related entity as having been
   * deleted from the database.  The relationship must have previously existed.
   *
   * @param entityId The entity ID of the first entity in the relationship.
   * @param relatedId The entity ID of the second entity in the relationship.
   * @param oldMatchType The non-null previous {@link SzMatchType} stored in
   *                     the data mart database.
   * @param oldSourceSummary The non-null {@link Map} of {@link String} data
   *                          source keys to {@link Integer} values providing
   *                          the data source record summary of the entity
   *                          identified by <code>entityId</code> previously
   *                          associated with the relationship.
   * @param oldRelatedSummary The non-null {@link Map} of {@link String} data
   *                           source keys to {@link Integer} values providing
   *                           the data source record summary of the entity
   *                           identified by <code>relatedId</code> previously
   *                           associated with the relationship.
   * @throws NullPointerException If a parameter is unexpectedly
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified entity ID's are not
   *                                  different or neither matches the entity ID
   *                                  for this instance.
   */
  public void deletedRelationship(long                  entityId,
                                  long                  relatedId,
                                  SzMatchType           oldMatchType,
                                  Map<String, Integer>  oldSourceSummary,
                                  Map<String, Integer>  oldRelatedSummary)
      throws NullPointerException, IllegalArgumentException
  {
    // get our entity ID
    long myEntityId = this.getEntityId();

    Objects.requireNonNull(
        oldSourceSummary,
        "Previous source summary cannot be null if deleted.  "
            + "entityId=[ " + entityId + " ], relatedId=[ " + relatedId + " ]");
    Objects.requireNonNull(
        oldRelatedSummary,
        "Previous related source summary cannot be null if deleted.  "
            + "entityId=[ " + entityId + " ], relatedId=[ " + relatedId + " ]");

    // check that our entity ID is one of the specified entity ID's
    if (entityId != myEntityId && relatedId != myEntityId) {
      throw new IllegalArgumentException(
          "Neither of the specified entity ID's match the entity ID for this "
              + "instance.  entityId=[ " + entityId + " ], relatedId=[ " + relatedId
              + " ], expectedId=[ " + myEntityId + " ]");
    }

    Objects.requireNonNull(
        oldMatchType,
        "Previous match type cannot be null if deleted.  entityId=[ "
            + entityId + " ], relatedId=[ " + relatedId + " ]");

    // check both entity ID's are not equal
    if (entityId == relatedId) {
      throw new IllegalArgumentException(
          "Both the entity ID and the related entity ID cannot be the same: "
              + entityId);
    }

    // normalize everything with respect to this entity's perspective
    if (myEntityId == relatedId) {
      Map<String, Integer> temp = oldSourceSummary;
      oldSourceSummary          = oldRelatedSummary;
      oldRelatedSummary         = temp;
      relatedId                 = entityId;
      entityId                  = myEntityId;
    }

    // now check that the related entity ID is related to the entity
    if (!this.getRemovedRelations().containsKey(relatedId)) {
      throw new IllegalArgumentException(
          "Cannot delete the relationship to an entity for which the "
              + "relationship was not removed.  relatedId=[ " + relatedId
              + " ], removed=[ " + this.getRemovedRelations().keySet() + " ]");
    }

    // determine the previous cross-source pairs
    Map<List<String>, Integer> oldCrossSources = crossSourceSummaries(
        oldSourceSummary, oldRelatedSummary);

    // determine the lesser and greater entity ID
    long id1  = (entityId < relatedId) ? entityId : relatedId;
    long id2  = (entityId < relatedId) ? relatedId : entityId;

    // get the statistic
    SzReportStatistic statistic = STATISTIC_LOOKUP.get(oldMatchType);

    // handle decrementing the counts accordingly
    oldCrossSources.forEach((sourcePair, count) -> {
      String source2 = sourcePair.get(1);
      String source1 = sourcePair.get(0);

      SzReportCode reportCode = (source1.equals(source2))
          ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

      // delete all the old counts for this relationship
      reportUpdates.add(
          builder(
              reportCode, statistic, source1, source2, id1, id2)
              .relations(-1).recordRelations(-1 * count).build());
    });
  }

  /**
   * Get all required report updates that have been detected so far.
   *
   * @return The {@link List} of {@link SzReportUpdate} instances describing
   *         the updates detected thus far.
   */
  public List<SzReportUpdate> getReportUpdates() {
    return Collections.unmodifiableList(this.reportUpdates);
  }

  /**
   * Gets the {@link Map} of {@link String} data source codes to {@link
   * Integer} record counts describing the data source summary for the specified
   * {@link SzEntity}.  If the specified {@link SzEntity} is <code>null</code>
   * then this returns an empty {@link Map}.
   *
   * @param entity The {@link SzEntity} from which to obtain the data source
   *               summary.
   *
   * @return The {@link Map} of {@link String} data source codes to {@link
   *         Integer} record counts describing the data source summary for the
   *         specified {@link SzEntity}, or an empty {@link Map} if the
   *         specified {@link SzEntity} is <code>null</code>.
   */
  private static Map<String, Integer> getSourceSummary(SzEntity entity)
  {
    return (entity == null) ? Collections.emptyMap()
        : entity.getSourceSummary();

  }

  /**
   * Gets the {@link Set} of {@link SzRecord} instances describing the records
   * for the specified {@link SzEntity}.  If the specified {@link SzEntity} is
   * <code>null</code> then this returns an empty {@link Map}.
   *
   * @param entity The {@link SzEntity} from which obtain the data sources.
   *
   * @return The {@link Set} of {@link SzRecord} instances describing the
   *         records for the specified {@link SzEntity}, or an empty {@link Set}
   *         if the specified {@link SzEntity} is <code>null</code>.
   */
  private static Set<SzRecord> getRecords(SzEntity entity) {
    return (entity == null) ? Collections.emptySet()
        : new LinkedHashSet<>(entity.getRecords());
  }

  /**
   * Gets the entity ID associated with the specified old and new {@link
   * SzEntity} instances, at least one of which must <b>not</b> be
   * <code>null</code>.  It is assumed that both describe the same entity at
   * different points in time if both are non-null.
   *
   * @param oldEntity The {@link SzEntity} describing the old state of the
   *                  entity, or <code>null</code> if the entity did not
   *                  previously exist in the data mart.
   * @param newEntity The {@link SzEntity} describing the new state of the
   *                  entity, or <code>null</code> if the entity no longer
   *                  exists in the entity repository.
   * @return The associated entity ID.
   * @throws IllegalArgumentException If both parameters are <code>null</code>.
   */
  private static long getEntityId(SzEntity oldEntity, SzEntity newEntity) {
    if (oldEntity != null) return oldEntity.getEntityId();
    if (newEntity != null) return newEntity.getEntityId();
    throw new IllegalArgumentException("Both arguments cannot be null.");
  }

  /**
   * Gets the {@link Map} of {@link Long} entity ID keys to {@link
   * SzRelatedEntity} values describing the related entities for the specified
   * {@link SzEntity}.  If the specified {@link SzEntity} is <code>null</code>
   * then an empty {@link Map} is returned.
   *
   * @param entity The {@link SzEntity} for which the related entities are
   *               being requested.
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the related entities for the
   *         specified {@link SzEntity}, or an empty {@link Map} if the
   *         specified {@link SzEntity} is <code>null</code>.
   */
  private static Map<Long, SzRelatedEntity> getRelatedEntities(
      SzResolvedEntity entity)
  {
    return (entity == null) ? Collections.emptyMap()
        : entity.getRelatedEntities();
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
  private static Set<SzRecord> findAddedRecords(Set<SzRecord> oldRecords,
                                                Set<SzRecord> newRecords)
  {
    Set<SzRecord> records = new LinkedHashSet<>();
    newRecords.forEach(record -> {
      if (!oldRecords.contains(record)) {
        records.add(record);
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
  private static Set<SzRecord> findRemovedRecords(Set<SzRecord>  oldRecords,
                                                  Set<SzRecord>  newRecords)
  {
    Set<SzRecord> records = new LinkedHashSet<>();
    oldRecords.forEach(record -> {
      if (!newRecords.contains(record)) {
        records.add(record);
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
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the newly added relationships.
   */
  private static Map<Long, SzRelatedEntity> findAddedRelations(
      Map<Long, SzRelatedEntity>  oldRelations,
      Map<Long, SzRelatedEntity>  newRelations)
  {
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
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the removed relationships.
   */
  private static Map<Long, SzRelatedEntity> findRemovedRelations(
      Map<Long, SzRelatedEntity>  oldRelations,
      Map<Long, SzRelatedEntity>  newRelations)
  {
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
   * specified {@link Map}'s describing the old and new relationships.  All
   * the relationships in the returned {@link Map} were present in the {@link
   * Map} of old relations, but also exist and are different in the new
   * relations.  In some cases, the only difference is the data sources have
   * changed for the entity.
   *
   * @param oldDataSources The {@link Set} of {@link String} data source codes
   *                       describing the data sources that the entity
   *                       previously had.
   *
   * @param newDataSources The {@link Set} of {@link String} data source codes
   *                       describing the data sources that the entity now has.
   *
   * @param oldRelations The {@link Map} of {@link Long} entity ID keys to
   *                     {@link SzRelatedEntity} instances describing the
   *                     relationships that entity previous had.
   *
   * @param newRelations The {@link Map} of {@link Long} entity ID keys to
   *                     {@link SzRelatedEntity} instances describing the
   *                     relationships that the entity now has.
   *
   * @return The {@link Map} of {@link Long} entity ID keys to {@link
   *         SzRelatedEntity} values describing the entity relationships that
   *         have changed.
   */
  private static Map<Long, SzRelatedEntity> findChangedRelations(
      Set<String>                   oldDataSources,
      Set<String>                   newDataSources,
      Map<Long, SzRelatedEntity>    oldRelations,
      Map<Long, SzRelatedEntity>    newRelations)
  {
    boolean dataSourcesChanged = (! oldDataSources.equals(newDataSources));

    Map<Long, SzRelatedEntity> relations = new LinkedHashMap<>();

    newRelations.forEach((entityId, newRelated) -> {
      // check if the relationship previously existed
      SzRelatedEntity oldRelated = oldRelations.get(entityId);
      if (oldRelated == null) return;

      // check if the data sources changed for this entity
      if (dataSourcesChanged) {
        relations.put(newRelated.getEntityId(), newRelated);
      }

      // check if the data sources changed for the other entity
      Map<String, Integer> oldRelatedSourceBreakdown
          = oldRelated.getSourceSummary();
      Map<String, Integer> newRelatedSourceBreakdown
          = newRelated.getSourceSummary();

      Set<String> oldRelatedSources = oldRelatedSourceBreakdown.keySet();
      Set<String> newRelatedSources = newRelatedSourceBreakdown.keySet();

      if (!oldRelatedSources.equals(newRelatedSources)) {
        relations.put(newRelated.getEntityId(), newRelated);
      }

      // check if other aspects of the related entity have changed
      if ((oldRelated.getMatchLevel() != newRelated.getMatchLevel())
          || (oldRelated.getMatchType() != newRelated.getMatchType())
          || (!oldRelated.getMatchKey().equals(newRelated.getMatchKey())))
      {
        relations.put(newRelated.getEntityId(), newRelated);
      }
    });

    return relations;
  }

  /**
   * Finds the changes to the {@link SzReportCode#ENTITY_SIZE_BREAKDOWN} report.
   *
   * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances.
   * @param oldEntity The old version of the entity, which may be
   *                  <code>null</code>.
   * @param newEntity The new version of the entity, which may be
   *                  <code>null</code>.
   * @param oldRecords The {@link Set} of {@link SzRecord} instances
   *                   describing the old records.
   * @param newRecords The {@link Set} of {@link SzRecord} instances
   *                   describing the new records.
   */
  private void findEntitySizeChanges(List<SzReportUpdate> reportUpdates,
                                     SzResolvedEntity   oldEntity,
                                     SzResolvedEntity   newEntity,
                                     Set<SzRecord>      oldRecords,
                                     Set<SzRecord>      newRecords)
  {
    int oldSize = oldRecords.size();
    int newSize = newRecords.size();
    if (oldSize == newSize) return;

    long entityId = getEntityId(oldEntity, newEntity);

    reportUpdates.add(
        builder(
            ENTITY_SIZE_BREAKDOWN, oldSize, entityId).entities(-1).build());

    reportUpdates.add(
        builder(
            ENTITY_SIZE_BREAKDOWN, newSize, entityId).entities(1).build());
  }

  /**
   * Finds the changes to the {@link SzReportCode#ENTITY_SIZE_BREAKDOWN} report.
   *
   * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances.
   * @param oldEntity The old version of the entity, which may be
   *                  <code>null</code>.
   * @param newEntity The new version of the entity, which may be
   *                  <code>null</code>.
   * @param oldRelations The {@link Map} of {@link Long} entity ID's to
   *                     {@link SzRelatedEntity} values describing the old
   *                     relationships.
   * @param newRelations The {@link Map} of {@link Long} entity ID's to
   *                     {@link SzRelatedEntity} values describing the new
   *                     relationships.
   */
  private void findEntityRelationChanges(
      List<SzReportUpdate>        reportUpdates,
      SzResolvedEntity            oldEntity,
      SzResolvedEntity            newEntity,
      Map<Long, SzRelatedEntity>  oldRelations,
      Map<Long, SzRelatedEntity>  newRelations)
  {
    int oldCount = oldRelations.size();
    int newCount = newRelations.size();
    if (oldCount == newCount) return;

    long entityId = getEntityId(oldEntity, newEntity);

    reportUpdates.add(
        builder(
            ENTITY_RELATION_BREAKDOWN, oldCount, entityId).entities(-1).build());

    reportUpdates.add(
        builder(
            ENTITY_RELATION_BREAKDOWN, newCount, entityId).entities(1).build());
  }

  /**
   * Finds the {@linkplain SzReportCode#DATA_SOURCE_SUMMARY data source summary}
   * report updates that can be determined without knowing the relationship
   * updates.
   *
   * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances
   *                      to be populated.
   * @param oldEntity The {@link SzEntity} describing the old state for the
   *                  entity.
   * @param newEntity The {@link SzEntity} describing the new state for the
   *                  entity.
   * @param oldSourceSummary The {@link Map} of {@link String} data source code
   *                         keys to {@link Integer} values indicating the
   *                         number of records previously from that source in
   *                         the entity.
   * @param newSourceSummary The {@link Map} of {@link String} data source code
   *                         keys to {@link Integer} values indicating the
   *                         number of records currently from that source in
   *                         the entity.
   */
  private static void findSourceSummaryChanges(
      List<SzReportUpdate>        reportUpdates,
      SzResolvedEntity            oldEntity,
      SzResolvedEntity            newEntity,
      Map<String, Integer>        oldSourceSummary,
      Map<String, Integer>        newSourceSummary)
  {
    long entityId = getEntityId(oldEntity, newEntity);

    // check the data source summary entities
    Map<String, Integer> addedSources = new LinkedHashMap<>(newSourceSummary);
    addedSources.keySet().removeAll(oldSourceSummary.keySet());

    Map<String, Integer> removedSources
        = new LinkedHashMap<>(oldSourceSummary);
    removedSources.keySet().removeAll(newSourceSummary.keySet());

    removedSources.forEach((source, recordCount) -> {

      reportUpdates.add(
          builder(
              DATA_SOURCE_SUMMARY, ENTITY_COUNT, source, source, entityId)
              .entities(-1).build());

      if (recordCount == 1) {
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, SINGLETON_COUNT, source, source, entityId)
                .entities(-1).records(-1).build());
      } else {
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, DUPLICATE_COUNT, source, source, entityId)
                .entities(-1).records(-1 * recordCount).build());
      }
    });

    addedSources.forEach((source, recordCount) -> {
      reportUpdates.add(
          builder(
              DATA_SOURCE_SUMMARY, ENTITY_COUNT, source, source, entityId)
              .entities(1).build());

      if (recordCount == 1) {
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, SINGLETON_COUNT, source, source, entityId)
                .entities(1).records(1).build());
      } else {
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, DUPLICATE_COUNT, source, source, entityId)
                .entities(1).records(recordCount).build());
      }
    });

    // now figure out the changed ones
    newSourceSummary.forEach((source, newCount) -> {
      Integer oldCount = oldSourceSummary.get(source);
      if (oldCount == null || newCount == oldCount) return;
      int diff = newCount - oldCount;

      // NOTE: no change to the entity count

      if (oldCount == 1) {
        // it was PREVIOUSLY a singleton
        // one less singleton
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, SINGLETON_COUNT, source, source, entityId)
                .entities(-1).records(-1).build());

        // one more duplicate
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, DUPLICATE_COUNT, source, source, entityId)
                .entities(1).records(newCount).build());

      } else if (newCount == 1) {
        // it is NOW a singleton
        // one more singleton
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, SINGLETON_COUNT, source, source, entityId)
                .entities(1).records(1).build());

        // one less duplicate
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, DUPLICATE_COUNT, source, source, entityId)
                .entities(-1).records(oldCount).build());
      } else {
        // it is a duplicate of different size
        reportUpdates.add(
            builder(
                DATA_SOURCE_SUMMARY, DUPLICATE_COUNT, source, source, entityId)
                .records(diff).build());

      }
    });
  }

  /**
   * Finds the {@linkplain SzReportCode#CROSS_SOURCE_SUMMARY cross source
   * summary} report updates that can be determined without knowing the
   * relationship updates.
   *
   * @param reportUpdates The {@link List} of {@link SzReportUpdate} instances
   *                      to be populated.
   * @param oldEntity The {@link SzEntity} describing the old state for the
   *                  entity.
   * @param newEntity The {@link SzEntity} describing the new state for the
   *                  entity.
   * @param oldSourceSummary The {@link Map} of {@link String} data source code
   *                         keys to {@link Integer} values indicating the
   *                         number of records previously from that source in
   *                         the entity.
   * @param newSourceSummary The {@link Map} of {@link String} data source code
   *                         keys to {@link Integer} values indicating the
   *                         number of records currently from that source in
   *                         the entity.
   */
  private static void findCrossSummaryChanges(
      List<SzReportUpdate>        reportUpdates,
      SzResolvedEntity            oldEntity,
      SzResolvedEntity            newEntity,
      Map<String, Integer>        oldSourceSummary,
      Map<String, Integer>        newSourceSummary)
  {
    long entityId = getEntityId(oldEntity, newEntity);

    // determine the old cross sources
    Map<List<String>, Integer> oldCrossSources = new LinkedHashMap<>();
    oldSourceSummary.forEach((source1, recordCount) -> {
      oldSourceSummary.keySet().forEach(source2 -> {
        oldCrossSources.put(List.of(source1, source2), recordCount);
      });
    });

    // determine the new cross sources
    Map<List<String>, Integer> newCrossSources = new LinkedHashMap<>();
    newSourceSummary.forEach((source1, recordCount) -> {
      newSourceSummary.keySet().forEach(source2 -> {
        newCrossSources.put(List.of(source1, source2), recordCount);
      });
    });

    // determine the added cross-source pairs
    Map<List<String>, Integer> addedPairs
        = new LinkedHashMap<>(newCrossSources);
    addedPairs.keySet().removeAll(oldCrossSources.keySet());

    Map<List<String>, Integer> removedPairs
        = new LinkedHashMap<>(oldCrossSources);
    removedPairs.keySet().removeAll(newCrossSources.keySet());

    // iterate over the removed pairs of data sources
    removedPairs.forEach((sources, recordCount) -> {
      String source1 = sources.get(0);
      String source2 = sources.get(1);
      reportUpdates.add(
          builder(
              CROSS_SOURCE_SUMMARY, MATCHED_COUNT, source1, source2, entityId)
              .entities(-1).records(-1 * recordCount).build());
    });

    // iterated over the added pairs of data sources
    addedPairs.forEach((sources, recordCount) -> {
      String source1 = sources.get(0);
      String source2 = sources.get(1);

      reportUpdates.add(
          builder(
              CROSS_SOURCE_SUMMARY, MATCHED_COUNT, source1, source2, entityId)
              .entities(1).records(recordCount).build());
    });

    // iterate over the pairs that exist in both maps
    newCrossSources.forEach((sources, newCount) -> {
      Integer oldCount = oldCrossSources.get(sources);
      if (oldCount == null) return;
      int diff = newCount - oldCount;
      String source1 = sources.get(0);
      String source2 = sources.get(1);

      reportUpdates.add(
          builder(
              CROSS_SOURCE_SUMMARY, MATCHED_COUNT, source1, source2, entityId)
              .records(diff).build());
    });
  }

  /**
   * Cross the data source summaries of an entity and a related entity into
   * a {@link Map} of data source code pairs.  When the data sources are the
   * same, then the record counts are added in the resulted {@link Map}, and
   * when they differ the count is taken from the first data source code in the
   * pair.
   *
   * @param summary1 The first {@link Map} of {@link String} data source code
   *                 keys to {@link Integer} values describing the record counts
   *                 per data source.
   * @param summary2 The second {@link Map} of {@link String} data source code
   *                 keys to {@link Integer} values describing the record counts
   *                 per data source.
   *
   * @return The {@link Map} of {@link List} values containing exactly two
   *         {@link String} data source codes and {@link Integer} values
   *         representing the total number of records for the first data source
   *         (which requires summing both values when they are the same source).
   */
  private static Map<List<String>, Integer> crossSourceSummaries(
      Map<String, Integer> summary1,
      Map<String, Integer> summary2)
  {
    Map<List<String>, Integer> result = new LinkedHashMap<>();

    summary1.forEach((source1, count1) -> {
      summary2.forEach((source2, count2) -> {
        if (source1.equals(source2)) {
          // count the record to record relations, the reverse relations
          // are the same if the sources are equal
          result.put(List.of(source1, source2), count1 * count2);

        } else {
          // get the reverse counts from the related entity (if any)
          Integer revCount1 = summary2.get(source1);
          Integer revCount2 = summary1.get(source2);
          if (revCount1 == null) revCount1 = 0;
          if (revCount2 == null) revCount2 = 0;

          // since the sources differ the reverse relations are distinct,
          // so add the forward record relations to the reverse ones
          int recordRelations = (count1 * count2) + (revCount1 * revCount2);

          // record the record relations for the pair and reverse pair
          result.put(List.of(source1, source2), recordRelations);
          result.put(List.of(source2, source1), recordRelations);
        }
      });
    });

    // return the result
    return result;
  }

  /**
   * Determines the delta counts between the old and the new maps.
   *
   * @param oldCrossSource The {@link Map} of cross-source pair keys to the
   *                       old counts as {@Integer} values.
   * @param newCrossSource The {@link Map} of cross-source pair keys to the
   *                       new counts as {@link Integer} values.
   * @return The {@link Map} of cross-source pair keys to {@link Integer} delta
   *         values.
   */
  private static Map<List<String>, Integer> diffCrossSourceSummaries(
      Map<List<String>, Integer> oldCrossSource,
      Map<List<String>, Integer> newCrossSource)
  {
    Map<List<String>, Integer> result = new LinkedHashMap<>();

    oldCrossSource.forEach((sourcePair, oldCount) -> {
      // get the new count
      Integer newCount = newCrossSource.get(sourcePair);

      // if no new count, then default to zero
      if (newCount == null) newCount = 0;

      // put the difference in the map
      result.put(sourcePair, newCount - oldCount);
    });
    newCrossSource.forEach((sourcePair, newCount) -> {
      // get the old count
      Integer oldCount = oldCrossSource.get(sourcePair);

      // check if already handled
      if (oldCount != null) return;

      // put the difference in the map
      result.put(sourcePair, newCount - oldCount);
    });

    return result;
  }

}
