package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.*;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.g2.G2Service;
import com.senzing.listener.service.locking.ResourceKey;
import com.senzing.listener.service.scheduling.Scheduler;

import javax.json.JsonObject;
import java.sql.*;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;
import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.datamart.SzReplicationProvider.*;
import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.listener.service.AbstractListenerService.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides a handler for refreshing an affected entity.
 */
public class RefreshEntityHandler extends AbstractTaskHandler {
  /**
   * The parameter key for the entity ID.
   */
  public static final String ENTITY_ID_KEY = "ENTITY_ID";

  /**
   * The flags to use when retrieving the entity from the G2 repository.
   */
  private static final long ENTITY_FLAGS
      = G2_ENTITY_INCLUDE_ENTITY_NAME
      | G2_ENTITY_INCLUDE_RECORD_DATA
      | G2_ENTITY_INCLUDE_ALL_RELATIONS
      | G2_ENTITY_INCLUDE_RELATED_MATCHING_INFO
      | G2_ENTITY_INCLUDE_RELATED_RECORD_DATA;

  /**
   * Maps the {@link SzReportCode} to the {@link TaskAction} for updating that
   * report.
   */
  private static final Map<SzReportCode, TaskAction> UPDATE_ACTION_MAP
      = Map.of(DATA_SOURCE_SUMMARY, UPDATE_DATA_SOURCE_SUMMARY,
               CROSS_SOURCE_SUMMARY, UPDATE_CROSS_SOURCE_SUMMARY,
               ENTITY_SIZE_BREAKDOWN, UPDATE_ENTITY_SIZE_BREAKDOWN,
               ENTITY_RELATION_BREAKDOWN, UPDATE_ENTITY_RELATION_BREAKDOWN);


  /**
   * Constructs with the specified {@link SzReplicationProvider} to use to
   * access the data mart replicator functions.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   */
  public RefreshEntityHandler(SzReplicationProvider provider) {
    super(provider, REFRESH_ENTITY);
  }

  /**
   * Implemented to handle the {@link TaskAction#REFRESH_ENTITY} action by
   * updating the deltas to the entity and deferring aggregate report updates.
   * <p>
   * {@inheritDoc}
   */
  @Override
  protected void handleTask(Map<String, Object> parameters,
                            int                 multiplicity,
                            Scheduler           followUpScheduler)
      throws ServiceExecutionException
  {
    Connection        conn      = null;
    PreparedStatement ps        = null;
    try {
      conn = this.getConnection();

      // get the entity ID
      long entityId = ((Number) parameters.get(ENTITY_ID_KEY)).longValue();

      // get the G2Service
      G2Service g2Service = this.getG2Service();

      // get the entity
      String            jsonText  = g2Service.getEntity(entityId, ENTITY_FLAGS);
      JsonObject        jsonObj   = parseJsonObject(jsonText);
      SzResolvedEntity  newEntity = SzResolvedEntity.parse(jsonObj);

      logDebug("REFRESHING ENTITY: " + entityId,
              (newEntity == null) ? "--> DELETED" : newEntity.toString());

      String deleteOpId = (newEntity == null)
          ? this.generateOperationId(entityId) : null;

      // ensure the row and get the previous entity hash
      String entityHash = (newEntity == null)
          ? this.prepareEntityDelete(conn, entityId, deleteOpId)
          : this.ensureEntityRow(conn, newEntity);

      // check if the previous hash is empty string (i.e.: no changes)
      if (entityHash != null && entityHash.length() == 0) {
        logDebug("ENTITY HASHES MATCH (PRESUMABLY NO CHANGES): "
                     + entityId);

        // check if the entity exists
        if (newEntity != null) {
          // if so, ensure we have relationship integrity
          EntityDelta noDelta = new EntityDelta(newEntity, newEntity);
          this.ensureRelationIntegrity(
              conn, noDelta, followUpScheduler, new HashSet<>());
        }
        return;
      }
      logDebug("CHANGES DETECTED FOR ENTITY: " + entityId);

      // parse the old entity
      SzResolvedEntity oldEntity = SzResolvedEntity.parseHash(entityHash);

      // check if the entity in unchanged -- this is a double-check since the
      // hashes should have been the same before we got here
      if (Objects.equals(oldEntity, newEntity)) {
        logWarning(
            "Entity hashes were different, but no delta was found.",
                "ENTITY ID : " + entityId,
                "NEW HASH  : " + newEntity.toHash(),
                "OLD HASH  : " + entityHash);
        return;
      }

      // find the entity deltas
      EntityDelta entityDelta = new EntityDelta(oldEntity, newEntity);

      logDebug("ENTITY " + entityId + " ADDED RELATIONS: ",
                   entityDelta.getAddedRelations());

      logDebug("ENTITY " + entityId + " CHANGED RELATIONS: ",
               entityDelta.getChangedRelations());

      logDebug("ENTITY " + entityId + " REMOVED RELATIONS: ",
               entityDelta.getRemovedRelations());

      // first enroll any subordinate resource locking rows into the transaction
      // to ensure mutual exclusion while avoiding deadlocks -- this will ensure
      // mutual exclusion on the record and relationship rows
      this.enrollLockingRows(conn, entityDelta);

      // check for added records
      int changeCount = this.ensureAddedRecords(conn,
                                                entityDelta,
                                                followUpScheduler);

      // check for removed records
      changeCount += this.orphanRemovedRecords(conn,
                                               entityDelta,
                                               followUpScheduler);

      // check for added relations
      changeCount += this.ensureRelations(conn, entityDelta, followUpScheduler);

      // get all the report updates and insert them as pending
      changeCount += this.insertReportDeltaUpdates(conn, entityDelta);

      // delete the entity row if deleted
      if (newEntity == null) {
        this.deleteEntityRow(conn, entityId, deleteOpId);
      }

      // notify the provider of report updates pending
      this.followUpOnReports(entityDelta);

      // commit the transaction -- this will release any locked rows
      conn.commit();

      // commit the follow-up scheduler
      followUpScheduler.commit();

    } catch (Exception e) {
      logError(e, "UNEXPECTED FAILURE -- ROLLING BACK TRANSACTION....");
      // rollback the transaction
      try {
        conn.rollback();
      } catch (Exception e2) {
        logError(e2, "FAILED TO ROLLBACK: ");
        e2.printStackTrace();
      }
      throw new ServiceExecutionException(e);

    } finally {
      ps = close(ps);
      conn = close(conn);
    }
  }

  /**
   * Prepares the entity row for deletion and retrieves the current entity
   * state from the previous hash using the specified JDBC {@link Connection},
   * specified entity ID and unique operation ID that can later be used to
   * actually delete the entity row.  This returns <code>null</code> if the
   * entity row is already deleted.
   *
   * @param conn        The JDBC {@link Connection} to use.
   * @param entityId    The entity ID for the entity.
   * @param operationId The unique operation ID previously used to prepare
   *                    the entity row for delete.
   * @return The {@link String} entity hash describing the previous state, or
   *         <code>null</code> if not found.
   * @throws SQLException If a failure occus.
   */
  protected String prepareEntityDelete(Connection conn,
                                       long       entityId,
                                       String     operationId)
      throws SQLException {
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = conn.prepareStatement(
          "UPDATE sz_dm_entity SET modifier_id = ? WHERE entity_id = ?");
      ps.setString(1, operationId);
      ps.setLong(2, entityId);

      int rowCount = ps.executeUpdate();

      // check if no rows were updated (i.e.: already deleted)
      if (rowCount == 0) {
        return "";
      }
      if (rowCount > 1) {
        throw new IllegalStateException(
            "Too many entity rows updated: " + rowCount);
      }

      ps = close(ps);

      ps = conn.prepareStatement(
          "SELECT entity_hash FROM sz_dm_entity "
              + "WHERE entity_id = ? AND modifier_id = ?");

      ps.setLong(1, entityId);
      ps.setString(2, operationId);

      rs = ps.executeQuery();

      if (!rs.next()) {
        throw new IllegalStateException(
            "Updated/locked row to prepare for delete and it was not found.");
      }

      // get the entity hash
      String entityHash = rs.getString(1);
      if (rs.wasNull()) entityHash = null;

      // release JDBC resources
      rs = close(rs);
      ps = close(ps);

      // return the entity hash
      return entityHash;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Deletes an entity row that was previously prepared for delete using the
   * specified JDBC {@link Connection}, entity ID and the operatioin ID
   * that was previously used to prepare the delete.
   *
   * @param conn        The JDBC {@link Connection} to use.
   * @param entityId    The entity ID for the entity.
   * @param operationId The unique operation ID previously used to prepare
   *                    the entity row for delete.
   * @throws SQLException If a failure occurs.
   */
  protected void deleteEntityRow(Connection conn,
                                 long       entityId,
                                 String     operationId)
      throws SQLException {
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(
          "DELETE FROM sz_dm_entity "
          + "WHERE entity_id = ? AND modifier_id = ?");
      ps.setLong(1, entityId);
      ps.setString(2, operationId);

      int rowCount = ps.executeUpdate();

      // check if no rows were updated (i.e.: already deleted)
      if (rowCount == 0) {
        throw new IllegalStateException(
            "Failed to find entity row to deleted.  entityId=[ "
                + entityId + " ], modifierId=[ " + operationId + " ]");
      }
      if (rowCount > 1) {
        throw new IllegalStateException(
            "Too many entity rows deleted: " + rowCount);
      }

      ps = close(ps);

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Attempts to insert or update the entity row.
   *
   * @param conn      The JDBC {@link Connection} to use.
   * @param newEntity The {@link SzResolvedEntity} describing the entity.
   * @return The previous entity hash if updated, <code>null</code> if inserted,
   * or empty-string if nothing was changed because the entity hashes
   * were the same.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected String ensureEntityRow(Connection conn, SzResolvedEntity newEntity)
      throws SQLException {
    PreparedStatement ps = null;
    ResultSet rs = null;
    String operationId = this.generateOperationId(newEntity.getEntityId());

    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_entity AS t1 ("
              + " entity_id, entity_name, record_count, relation_count, "
              + " entity_hash, creator_id, modifier_id) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?) "
              + "ON CONFLICT (entity_id) DO UPDATE SET"
              + " entity_name = EXCLUDED.entity_name,"
              + " record_count = EXCLUDED.record_count,"
              + " relation_count = EXCLUDED.relation_count,"
              + " entity_hash = EXCLUDED.entity_hash,"
              + " prev_entity_hash = t1.entity_hash,"
              + " modifier_id = EXCLUDED.modifier_id "
              + "WHERE t1.entity_hash <> EXCLUDED.entity_hash");

      ps.setLong(1, newEntity.getEntityId());
      ps.setString(2, newEntity.getEntityName());
      ps.setInt(3, newEntity.getRecords().size());
      ps.setInt(4, newEntity.getRelatedEntities().size());
      ps.setString(5, newEntity.toHash());
      ps.setString(6, operationId);
      ps.setString(7, operationId);

      int rowCount = ps.executeUpdate();

      ps = close(ps);

      // check if nothing was updated or inserted (only happens if no patches)
      if (rowCount == 0) return "";

      if (rowCount > 1) {
        throw new IllegalStateException(
            "Too many entity rows updated: " + rowCount);
      }

      // check if a row was updated or inserted
      ps = conn.prepareStatement(
          "SELECT prev_entity_hash "
              + "FROM sz_dm_entity WHERE entity_id = ? AND modifier_id = ?");
      ps.setLong(1, newEntity.getEntityId());
      ps.setString(2, operationId);

      rs = ps.executeQuery();

      if (!rs.next()) {
        throw new IllegalStateException(
            "Updated an entity row and could not find it by modifier_id.  "
                + "operationId=[ " + operationId + " ], entity=[ "
                + newEntity + " ]");
      }

      // get the previous entity hash
      String prevEntityHash = rs.getString(1);
      if (rs.wasNull()) prevEntityHash = null;

      // release JDBC resources
      rs = close(rs);
      ps = close(ps);

      // return the previous entity hash
      return prevEntityHash;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Ensures the added record rows exist in the database and that their
   * entity ID's are set to that from the specified {@link EntityDelta}.
   * Additionally, this will track all rows that were actually inserted
   * rather than updated against using the {@link
   * EntityDelta#createdRecord(SzRecord)} function on the specified {@link
   * EntityDelta}.
   *
   * @param conn              The JDBC {@link Connection} to use.
   * @param entityDelta       The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @return The number of records actually created.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int ensureAddedRecords(Connection   conn,
                                   EntityDelta  entityDelta,
                                   Scheduler    followUpScheduler)
      throws SQLException {
    Set<SzRecord> addedRecords = entityDelta.getAddedRecords();
    if (addedRecords.size() == 0) return 0;

    PreparedStatement ps = null;
    ResultSet rs = null;
    String operationId = this.generateOperationId();
    int createdCount = 0;
    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_record AS t1 ("
              + " data_source, record_id, entity_id, creator_id, modifier_id) "
              + "VALUES (?, ?, ?, ?, ?) "
              + "ON CONFLICT (data_source, record_id) DO UPDATE SET"
              + " entity_id = EXCLUDED.entity_id,"
              + " modifier_id = EXCLUDED.modifier_id, "
              + " adopter_id = (CASE WHEN (t1.entity_id = 0) "
              + "THEN (EXCLUDED.modifier_id) ELSE(NULL) END)");

      this.batchUpdate(ps, addedRecords, (ps2, record) -> {
        ps2.setString(1, record.getDataSource());
        ps2.setString(2, record.getRecordId());
        ps2.setLong(3, entityDelta.getEntityId());
        ps2.setString(4, operationId);
        ps2.setString(5, operationId);
        return 1;
      });

      ps = close(ps);

      // now get the created records -- we don't need the modified ones
      ps = conn.prepareStatement(
          "SELECT data_source, record_id "
              + "FROM sz_dm_record WHERE entity_id = ? AND creator_id = ?");

      ps.setLong(1, entityDelta.getEntityId());
      ps.setString(2, operationId);

      rs = ps.executeQuery();
      while (rs.next()) {
        String dataSource = rs.getString(1);
        String recordId   = rs.getString(2);

        SzRecord record = new SzRecord(dataSource, recordId);
        // flag the record as created (even if it was adopted)
        entityDelta.createdRecord(record);
        createdCount++;
      }

      rs = close(rs);
      ps = close(ps);

      return createdCount;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Handles changing the entity ID of any removed records to be zero (0)
   * providing the entity ID is still the same as that for the specified
   * {@link EntityDelta}.  This will mark records as {@linkplain
   * EntityDelta#orphanedRecord(SzRecord) orphaned} in the specified
   * {@link EntityDelta} if the record row was in fact updated.
   *
   * @param conn              The JDBC {@link Connection} to use.
   * @param entityDelta       The {@link EntityDelta} to use to obtain the removed
   *                          records.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @return The number of records actually modified (i.e.: those that have
   * not already been claimed by other entities).
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int orphanRemovedRecords(Connection   conn,
                                     EntityDelta  entityDelta,
                                     Scheduler    followUpScheduler)
      throws SQLException
  {
    Set<SzRecord> removedRecords = entityDelta.getRemovedRecords();
    if (removedRecords.size() == 0) return 0;

    PreparedStatement ps = null;
    String operationId = this.generateOperationId();
    try {
      ps = conn.prepareStatement(
          "UPDATE sz_dm_record SET entity_id = 0, modifier_id = ? "
              + "WHERE data_source = ? AND record_id = ? AND entity_id = ?");

      List<Integer> rowCounts
          = this.batchUpdate(ps, removedRecords, (ps2, record) -> {
        ps2.setString(1, operationId);
        ps2.setString(2, record.getDataSource());
        ps2.setString(3, record.getRecordId());
        ps2.setLong(4, entityDelta.getEntityId());
        return -1;
      });

      // close the statement
      ps = close(ps);

      int expectedCount = sum(rowCounts);

      // now get the modified records
      ps = conn.prepareStatement(
          "SELECT data_source, record_id"
              + " FROM sz_dm_record WHERE entity_id = 0 AND modifier_id = ?");

      ps.setString(1, operationId);

      ResultSet rs = ps.executeQuery();
      int orphanedCount = 0;
      while (rs.next()) {
        orphanedCount++;

        String    dataSource  = rs.getString(1);
        String    recordId    = rs.getString(2);
        SzRecord  record      = new SzRecord(dataSource, recordId);

        entityDelta.orphanedRecord(record);
      }

      rs = close(rs);
      ps = close(ps);

      if (orphanedCount != expectedCount) {
        throw new IllegalStateException(
            "Orphaned record count (" + orphanedCount + ") for entity "
                + entityDelta.getEntityId() + " was not as expected ("
                + expectedCount + ").  Likely race condition.");
      }

      return orphanedCount;

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Ensures the added and changed relationship rows exist in the database with
   * the correct match type and data source summaries and that the removed
   * relations are deleted.  Additionally, this will track all rows that were
   * actually inserted or updated using the {@link
   * EntityDelta#trackStoredRelationship(long, long, SzMatchType, Map, Map)}
   * function on the specified {@link EntityDelta} and all rows that were
   * deleted using the {@link EntityDelta#trackDeletedRelationship(long, long,
   * SzMatchType, Map, Map)} method.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   *
   * @return The number of relationships modified or created.
   * @throws SQLException If a JDBC failure occurs.
   *
   */
  protected int ensureRelations(Connection  conn,
                                EntityDelta entityDelta,
                                Scheduler   followUpScheduler)
      throws SQLException
  {
    Set<Long> followUpSet = new LinkedHashSet<>();
    int       changeCount = 0;

    // any added relation requires a follow-up
    int addRelationChanges = this.ensureAddedRelations(
        conn, entityDelta, followUpScheduler, followUpSet);

    changeCount += addRelationChanges;

    // any removed relation requires a follow-up
    int removedRelationChanges = this.ensureRemovedRelations(
        conn, entityDelta, followUpScheduler, followUpSet);

    changeCount += removedRelationChanges;

    // check if we have stale relationships to follow-up on
    this.ensureRelationIntegrity(
        conn, entityDelta, followUpScheduler, followUpSet);

    // now check get the current and previous data sources for this entity
    Set<String> oldSources = entityDelta.getOldSourceSummary().keySet();
    Set<String> newSources = entityDelta.getNewSourceSummary().keySet();

    long entityId = entityDelta.getEntityId();

    // check if my sources have changed and refresh related entities
    logDebug("ENTITY " + entityId + " OLD/NEW SOURCES: ", oldSources,
             newSources);

    if (!oldSources.equals(newSources)) {
      // if the sources for this entity have changed, then every related entity
      // potentially may change its contributions to the related entity counts
      entityDelta.getOldRelatedEntities().keySet().forEach(
          relatedId -> {
            followUpOnRelatedEntity(
                followUpScheduler, followUpSet, entityId, relatedId);
          });

      entityDelta.getNewRelatedEntities().keySet().forEach(
          relatedId -> {
            followUpOnRelatedEntity(
                followUpScheduler, followUpSet, entityId, relatedId);
          });
    }

    // check if we followed up on anything related
    if (followUpSet.size() > 0) {
      // follow-up on this same entity in a bit simply to ensure that things
      // that might still be in motion have settled down
      followUpOnRelatedEntity(
          followUpScheduler, followUpSet, entityId, entityId);
    }

    // check if the
    return changeCount;
  }

  /**
   * Ensures the added and changed relationship rows exist in the database with
   * the correct match type and data source summaries.  Additionally, this will
   * track all rows that were actually inserted or updated using the {@link
   * EntityDelta#trackStoredRelationship(long, long, SzMatchType, Map, Map)}
   * function on the specified {@link EntityDelta}.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @param followUpSet The {@link Set} to populate with {@link Long} entity
   *                    ID's for which a relationship refresh follow-up was
   *                    scheduled.
   *
   * @return The number of relationships modified or created.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int ensureAddedRelations(Connection   conn,
                                     EntityDelta  entityDelta,
                                     Scheduler    followUpScheduler,
                                     Set<Long>    followUpSet)
      throws SQLException
  {
    Map<Long, SzRelatedEntity> relations = new LinkedHashMap<>();
    relations.putAll(entityDelta.getAddedRelations());
    relations.putAll(entityDelta.getChangedRelations());
    if (relations.size() == 0) return 0;

    List<SzRelationship> relationships = new ArrayList<>(relations.size());

    String operationId = this.generateOperationId(entityDelta.getEntityId());

    PreparedStatement ps = null;
    ResultSet         rs = null;

    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_relation AS t1 ("
              + " entity_id, related_id, match_level, match_key, match_type,"
              + " relation_hash, creator_id, modifier_id) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
              + "ON CONFLICT (entity_id, related_id) DO UPDATE SET"
              + " match_level = EXCLUDED.match_level,"
              + " match_key = EXCLUDED.match_key,"
              + " match_type = EXCLUDED.match_type,"
              + " relation_hash = EXCLUDED.relation_hash,"
              + " prev_relation_hash = t1.relation_hash,"
              + " modifier_id = EXCLUDED.modifier_id "
              + "WHERE t1.relation_hash <> EXCLUDED.relation_hash");

      List<Integer> rowCounts = this.batchUpdate(
          ps, relations.values(), (ps2, relatedEntity) -> {

        SzRelationship relationship
            = new SzRelationship(entityDelta.getNewEntity(), relatedEntity);

        relationships.add(relationship);

        ps2.setLong(1, relationship.getEntityId());
        ps2.setLong(2, relationship.getRelatedEntityId());
        ps2.setInt(3, relationship.getMatchLevel());
        ps2.setString(4, relationship.getMatchKey());
        ps2.setString(5, relationship.getMatchType().toString());
        ps2.setString(6, relationship.toHash());
        ps2.setString(7, operationId);
        ps2.setString(8, operationId);
        return -1;
      });

      ps = close(ps);

      // determine the expected number of rows to update
      int expectedCount = sum(rowCounts);

      // now get the ones that were actually added or modified along with their
      // previous relationship hashes
      ps = conn.prepareStatement(
          "SELECT COUNT (*) FROM sz_dm_relation WHERE modifier_id = ?");

      ps.setString(1, operationId);

      rs = ps.executeQuery();

      rs.next();
      int updateCount = rs.getInt(1);

      // check if the update count matched the expected count
      if (updateCount != expectedCount) {
        throw new IllegalStateException(
            "Did not update the expected number of relationship rows.  "
            + "expected=[ " + expectedCount + " ], updated=[ " + updateCount
            + " ], entityId=[ " + entityDelta.getEntityId()
                + " ], operationId=[ " + operationId
                + " ], relationships=[ " + relationships + " ]");
      }

      rs = close(rs);
      ps = close(ps);

      // now make sure to account for reporting on those we did not update
      for (SzRelationship relationship : relationships) {

        // get the entity ID and related entity ID
        long entityId = entityDelta.getEntityId();
        long relatedId = (entityId == relationship.getEntityId())
            ? relationship.getRelatedEntityId() : relationship.getEntityId();

        // get the entity and the related entity
        SzResolvedEntity entity = entityDelta.getOldEntity();

        SzRelatedEntity relatedEntity
            = entityDelta.getOldRelatedEntities().get(relatedId);

        // check how the relationship was previously defined
        SzRelationship prevRelation = (relatedEntity == null) ? null
            : new SzRelationship(entity, relatedEntity);

        SzMatchType prevMatchType = (prevRelation == null) ? null
            : prevRelation.getMatchType();

        Map<String, Integer> sourceSummary = (prevRelation == null) ? null
            : prevRelation.getSourceSummary();

        Map<String, Integer> relatedSummary = (prevRelation == null) ? null
            : prevRelation.getRelatedSourceSummary();

        // now check if they are different
        entityDelta.trackStoredRelationship(relationship.getEntityId(),
                                            relationship.getRelatedEntityId(),
                                            prevMatchType,
                                            sourceSummary,
                                            relatedSummary);

        // check if we need to schedule a follow-up
        if (prevRelation == null || (entityDelta.getOldEntity() == null)
            || prevRelation.getMatchType() != relationship.getMatchType())
        {
          followUpOnRelatedEntity(
              followUpScheduler, followUpSet, entityId, relatedId);
        }
      }

      // return the update count
      return updateCount;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Schedules a relationship refresh follow-up using the specified {@link
   * Scheduler} for the specified related entity ID using the specified
   * entity ID and {@link Set} of {@link Long} entity ID's for which follow-ups
   * have already be scheduled.
   *
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @param followUpSet The {@link Set} to populate with {@link Long} entity
   *                    ID's for which a relationship refresh follow-up was
   *                    scheduled.
   * @param entityId The entity ID of the entity we are refreshing.
   * @param relatedId The related entity ID that requires a refresh.
   */
  private static void followUpOnRelatedEntity(Scheduler followUpScheduler,
                                              Set<Long> followUpSet,
                                              long      entityId,
                                              long      relatedId)
  {
    if (followUpSet.contains(relatedId)) {
      return;
    }

    logDebug("ENTITY " + entityId + " FOLLOWING UP ON ENTITY "
                 + relatedId);
    followUpScheduler.createTaskBuilder(REFRESH_ENTITY.toString())
        .resource(ENTITY_RESOURCE_KEY, relatedId)
        .parameter(RefreshEntityHandler.ENTITY_ID_KEY, relatedId)
        .schedule(true);
    followUpSet.add(relatedId);
  }

  /**
   * Ensures the added and changed relationship rows exist in the database with
   * the correct match type and data source summaries.  Additionally, this will
   * track all rows that were actually inserted or updated using the {@link
   * EntityDelta#trackStoredRelationship(long, long, SzMatchType, Map, Map)}
   * function on the specified {@link EntityDelta}.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @param followUpSet The {@link Set} to populate with {@link Long} entity
   *                    ID's for which a relationship refresh follow-up was
   *                    scheduled.
   *
   * @return The number of relationships modified or created.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int ensureRemovedRelations(Connection   conn,
                                       EntityDelta  entityDelta,
                                       Scheduler    followUpScheduler,
                                       Set<Long>    followUpSet)
      throws SQLException
  {
    Map<Long, SzRelatedEntity> relations = entityDelta.getRemovedRelations();
    if (relations.size() == 0) return 0;

    List<SzRelationship> relationships = new ArrayList<>(relations.size());

    PreparedStatement ps = null;
    ResultSet         rs = null;

    String operationId = this.generateOperationId(entityDelta.getEntityId());

    try {
      ps = conn.prepareStatement(
          "UPDATE sz_dm_relation SET modifier_id = ? "
          + "WHERE entity_id = ? AND related_id = ?");

      List<Integer> rowCounts = this.batchUpdate(
          ps, relations.values(), (ps2, relatedEntity) -> {

            SzRelationship relationship
                = new SzRelationship(entityDelta.getOldEntity(), relatedEntity);

            relationships.add(relationship);

            ps2.setString(1, operationId);
            ps2.setLong(2, relationship.getEntityId());
            ps2.setLong(3, relationship.getRelatedEntityId());
            return -1;
          });

      ps = close(ps);

      // determine the expected number of rows to update
      int expectedCount = sum(rowCounts);

      // now get the ones that were actually added or modified along with their
      // current relationship hashes
      ps = conn.prepareStatement(
          "SELECT entity_id, related_id, relation_hash "
              + "FROM sz_dm_relation WHERE modifier_id = ?");

      ps.setString(1, operationId);

      rs = ps.executeQuery();

      List<SzRelationship> pendingDelete = new ArrayList<>(expectedCount);

      while (rs.next()) {
        long    entityId  = rs.getLong(1);
        long    relatedId = rs.getLong(2);
        String  hash      = rs.getString(3);

        SzRelationship relationship = SzRelationship.parseHash(hash);
        if (relationship == null) {
          throw new IllegalStateException(
              "Existing relationship exists but has no hash.  relationship=[ "
                  + relationship + " ]");
        }
        if ((relationship.getEntityId() != entityId)
            || (relationship.getRelatedEntityId() != relatedId))
        {
          throw new IllegalStateException(
              "Relationship from hash does not match expected entity ID ("
              + entityId + ") and related entity ID (" + relatedId + "): "
              + relationship);
        }
        pendingDelete.add(relationship);
      }

      // free JDBC resources
      rs = close(rs);
      ps = close(ps);

      // check the counts
      if (pendingDelete.size() != expectedCount) {
        throw new IllegalStateException(
            "Did not prepare the expected number of relationship rows for "
                + "deletion.  expected=[ " + expectedCount + " ], found=[ "
                + pendingDelete.size() + " ], entityId=[ "
                + entityDelta.getEntityId() + " ], operationId=[ "
                + operationId + " ], found=[ " + pendingDelete + " ]");
      }

      // now delete using batch update
      ps = conn.prepareStatement(
          "DELETE FROM sz_dm_relation "
              + "WHERE entity_id = ? AND related_id = ? AND modifier_id = ?");

      rowCounts = this.batchUpdate(
          ps, pendingDelete, (ps2, relationship) -> {

            ps2.setLong(1, relationship.getEntityId());
            ps2.setLong(2, relationship.getRelatedEntityId());
            ps2.setString(3, operationId);
            return -1;
          });

      ps = close(ps);

      // iterate over the row counts
      int index = 0, deletedCount = 0;
      for (SzRelationship relationship: pendingDelete) {
        int rowCount = rowCounts.get(index++);
        if (rowCount == 0) {
          throw new IllegalStateException(
              "Relationship was deleted externally despite lock. This may "
              + "cause reporting totals to be incorrect: " + relationship);
        }

        // increment the deleted count
        deletedCount++;
      }

      for (SzRelatedEntity relatedEntity: relations.values()) {
        SzRelationship relationship
            = new SzRelationship(entityDelta.getOldEntity(), relatedEntity);

        long relatedId
            = (relationship.getEntityId() == entityDelta.getEntityId())
            ? relationship.getRelatedEntityId() : relationship.getEntityId();

        long entityId = entityDelta.getEntityId();

        // mark the relationship as deleted
        entityDelta.trackDeletedRelationship(relationship.getEntityId(),
                                             relationship.getRelatedEntityId(),
                                             relationship.getMatchType(),
                                             relationship.getSourceSummary(),
                                             relationship.getRelatedSourceSummary());

        followUpOnRelatedEntity(
            followUpScheduler, followUpSet, entityId, relatedId);

      }

      return deletedCount;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Ensures the related entities connected by stale relationships to this
   * entity are followed up on and missing relationships in the mart are also
   * followed up on.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @param followUpSet The {@link Set} to populate with {@link Long} entity
   *                    ID's for which a relationship refresh follow-up was
   *                    scheduled.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void ensureRelationIntegrity(Connection   conn,
                                         EntityDelta  entityDelta,
                                         Scheduler    followUpScheduler,
                                         Set<Long>    followUpSet)
      throws SQLException
  {
    Set<Long> knownRelations = new LinkedHashSet<>();

    knownRelations.addAll(entityDelta.getOldRelatedEntities().keySet());
    knownRelations.addAll(entityDelta.getNewRelatedEntities().keySet());

    PreparedStatement ps = null;
    ResultSet         rs = null;

    long entityId = entityDelta.getEntityId();

    try {
      ps = conn.prepareStatement(
          "SELECT entity_id FROM sz_dm_relation WHERE related_id = ? "
              + "UNION "
              + "SELECT related_id FROM sz_dm_relation WHERE entity_id = ?");

      ps.setLong(1, entityId);
      ps.setLong(2, entityId);

      rs = ps.executeQuery();

      while (rs.next()) {
        long relatedId = rs.getLong(1);
        if (!knownRelations.contains(relatedId)) {
          logDebug("STALE RELATION FROM ENTITY " + entityId
                       + " TO ENTITY " + relatedId + " DETECTED");
          followUpOnRelatedEntity(
              followUpScheduler, followUpSet, entityId, relatedId);
        } else {
          // remove it from the set since it was found
          knownRelations.remove(relatedId);
        }
      }

      // at this point the set should be empty, if not following up on remaining
      if (knownRelations.size() > 0) {
        for (Long relatedId : knownRelations) {
          logDebug("MISSING RELATION FROM ENTITY " + entityId
                       + " TO ENTITY " + relatedId + " DETECTED");
          followUpOnRelatedEntity(
              followUpScheduler, followUpSet, entityId, relatedId);
        }
      }

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Iterates over the pending report updates and batch-insert them into the
   * repository for later handling.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param delta The {@link EntityDelta} that has accumulated the report
   *              updates.
   * @return The number of rows inserted.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int insertReportDeltaUpdates(Connection conn, EntityDelta delta)
    throws SQLException
  {
    // get the updates
    List<SzReportUpdate> updates = delta.getReportUpdates();
    if (updates.size() == 0) return 0;

    String operationId = this.generateOperationId(delta.getEntityId());

    PreparedStatement ps = null;

    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_pending_report AS t1 ("
              + " report_key, entity_delta, record_delta,"
              + " relation_delta, entity_id, related_id) "
              + "VALUES (?, ?, ?, ?, ?, ?)");

      List<Integer> rowCounts = this.batchUpdate(
          ps, updates, (ps2, update) -> {

            ps2.setString(1, update.getReportKey().toString());
            ps2.setInt(2, update.getEntityDelta());
            ps2.setInt(3, update.getRecordDelta());
            ps2.setInt(4, update.getRelationDelta());
            ps2.setLong(5, update.getEntityId());

            Long relatedId = update.getRelatedEntityId();
            if (relatedId == null) {
              ps2.setNull(6, Types.INTEGER);
            } else {
              ps2.setLong(6, relatedId);
            }
            return 1;
          });

      ps = close(ps);

      // sum the row counts
      int insertCount = sum(rowCounts);

      // verify the insert count
      if (insertCount != updates.size()) {
        throw new IllegalStateException(
            "Unexpected number of pending report update inserts.  expected=[ "
                + updates.size() + " ], actual=[ " + insertCount + " ]");
      }

      // return the insert count
      return insertCount;

    } finally {
      ps = close(ps);
    }
  }

  /**
   * This method handles notifying that we need to schedule report aggregation
   * follow-up tasks for the modified reports.
   *
   * @param entityDelta The {@link EntityDelta} to get the pending updates from.
   *
   * @return The number of unique follow-up tasks scheduled.
   */
  protected int followUpOnReports(EntityDelta entityDelta) {
    // get the updates
    List<SzReportUpdate>  updates     = entityDelta.getReportUpdates();
    Set<SzReportKey>      reportKeys  = new LinkedHashSet<>();
    for (SzReportUpdate update : updates) {
      reportKeys.add(update.getReportKey());
    }

    // now that we have a unique set of report keys, do the scheduling
    for (SzReportKey reportKey : reportKeys) {
      TaskAction action = UPDATE_ACTION_MAP.get(reportKey.getReportCode());
      this.scheduleReportFollowUp(action, reportKey);
    }

    // return the number of scheduled follow-ups
    return reportKeys.size();
  }

  /**
   * Enrolls the database rows for locking resources into the transaction in a
   * consistent order so that mutual exclusion is ensured while avoiding
   * deadlocks.
   *
   * @param conn The {@link Connection} to the database.
   * @param entityDelta The {@link EntityDelta} describing the changes.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void enrollLockingRows(Connection conn, EntityDelta entityDelta)
    throws SQLException
  {
    PreparedStatement ps  = null;

    String operationId = this.generateOperationId(entityDelta.getEntityId());

    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_locks AS t1 (resource_key, modifier_id) "
          + "VALUES (?, ?) "
          + "ON CONFLICT (resource_key) DO UPDATE SET"
          + " modifier_id = EXCLUDED.modifier_id");

      SortedSet<ResourceKey> resourceKeys = entityDelta.getResourceKeys();

      this.batchUpdate(ps, resourceKeys, (ps2, resourceKey) ->
          {
            ps2.setString(1, resourceKey.toString());
            ps2.setString(2, operationId);
            return 1;
          });

    } finally {
      ps = close(ps);
    }
  }
}
