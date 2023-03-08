package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.*;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.g2.G2Service;
import com.senzing.listener.service.locking.ResourceKey;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.util.JsonUtilities;
import com.senzing.util.LoggingUtilities;

import javax.json.JsonObject;
import javax.swing.text.DefaultEditorKit;
import java.sql.*;
import java.util.*;

import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.datamart.SzReplicationProvider.*;
import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.listener.service.AbstractListenerService.*;

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
   *
   * {@inheritDoc}
   */
  @Override
  protected void handleTask(Map<String, Object> parameters,
                            int                 multiplicity,
                            Scheduler           followUpScheduler)
      throws ServiceExecutionException
  {
    Connection        conn  = null;
    PreparedStatement ps    = null;
    try {
      conn = this.getConnection();

      // get the entity ID
      long entityId = ((Number) parameters.get(ENTITY_ID_KEY)).longValue();

      // get the G2Service
      G2Service g2Service = this.getG2Service();

      // get the entity
      String            jsonText  = g2Service.getEntity(entityId, ENTITY_FLAGS);
      JsonObject        jsonObj   = JsonUtilities.parseJsonObject(jsonText);
      SzResolvedEntity  newEntity = SzResolvedEntity.parse(jsonObj);

      // create a map for relationship patches
      Map<Long, Boolean> relPatchMap = new LinkedHashMap<>();

      // ensure the row and get the previous entity hash
      String entityHash = (newEntity == null)
          ? this.prepareEntityDelete(conn, entityId, relPatchMap)
          : this.ensureEntityRow(conn, newEntity, relPatchMap);

      // check if the previous hash is empty string (i.e.: no changes)
      if (entityHash != null && entityHash.length() == 0) {
        return;
      }

      // parse the old entity
      SzResolvedEntity oldEntity = SzResolvedEntity.parseHash(entityHash);

      // check if the entity in unchanged -- this is a double-check since the
      // hashes should have been the same before we got here
      if (Objects.equals(oldEntity, newEntity)) {
        System.err.println(
            LoggingUtilities.multilineFormat(
                "",
            "--------------------------------------------------------------",
            "WARNING: Entity hashes were different, but no delta was found.",
            "ENTITY ID : " + entityId,
            "NEW HASH  : " + newEntity.toHash(),
            "OLD HASH  : " + entityHash));
        return;
      }

      // find the entity deltas
      EntityDelta entityDelta
          = new EntityDelta(oldEntity, newEntity, relPatchMap);

      // count the number of actual changes
      int changeCount = 0;

      // first enroll any subordinate resource locking rows into the transaction
      // to ensure mutual exclusion while avoiding deadlocks -- this will ensure
      // mutual exclusion on the record and relationship rows
      this.enrollLockingRows(conn, entityDelta);

      // check for added records
      changeCount += this.ensureAddedRecords(conn,
                                             entityDelta,
                                             followUpScheduler);

      // check for removed records
      changeCount += this.orphanRemovedRecords(conn,
                                               entityDelta,
                                               followUpScheduler);

      // check for added relations
      changeCount += this.ensureCurrentRelations(conn,
                                                 entityDelta,
                                                 followUpScheduler);

      // check for removed relations
      changeCount += this.ensureRemovedRelations(conn,
                                                 entityDelta,
                                                 followUpScheduler);

      // warn if the change count is zero
      if (changeCount == 0) {
        System.err.println(
            LoggingUtilities.multilineFormat(
                "",
                "-------------------------------------------------------",
                "WARNING: Entity hashes were different, but no detail changes "
                + "were applied.",
                "This may occur if all changes were already applied by another "
                + "task.",
                "ENTITY ID          : " + entityId,
                "OLD RECORD COUNT   : " + entityDelta.getOldRecords().size(),
                "NEW RECORD COUNT   : " + entityDelta.getNewRecords().size(),
                "OLD RELATION COUNT : "
                    + entityDelta.getOldRelatedEntities().size(),
                "NEW RELATION COUNT : "
                    + entityDelta.getNewRelatedEntities().size()));
      }

      // get all the report updates and insert them as pending
      changeCount += this.insertReportDeltaUpdates(conn, entityDelta);

      // notify the provider of report updates pending
      this.followUpOnReports(entityDelta);

      // commit the transaction -- this will release any locked rows
      conn.commit();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("Rolling back transaction....");
      // rollback the transaction
      try {
        conn.rollback();
      } catch (Exception e2) {
        System.err.println("**** FAILED TO ROLLBACK");
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
   * state from the previous hash using the specified JDBC {@link Connection}
   * and specified entity ID.  This returns <code>null</code> if the entity row
   * is already deleted.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityId The entity ID for the entity.
   * @param relationPatchMap The {@link Map} of {@link Long} entity ID's to
   *                         {@link Boolean} values to populate to indicate how
   *                         the relationships had been patched since the
   *                         entity hash was stored.
   * @return The {@link SzResolvedEntity} describing the previous state, or
   *         <code>null</code> if not found.
   */
  protected String prepareEntityDelete(Connection         conn,
                                       long               entityId,
                                       Map<Long,Boolean>  relationPatchMap)
      throws SQLException
  {
    PreparedStatement ps          = null;
    ResultSet         rs          = null;
    String            operationId = this.generateOperationId(entityId);
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
          "SELECT entity_hash, relations_hash FROM sz_dm_entity "
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

      // get the relationship patch hash
      String relationsHash = rs.getString(2);
      if (rs.wasNull()) relationsHash = null;
      this.parsePatchHash(relationsHash, relationPatchMap);

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
   * Attempts to insert or update the entity row.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param newEntity The {@link SzResolvedEntity} describing the entity.
   * @param prevRelationPatchMap The {@link Map} of {@link Long} entity ID's
   *                             to {@link Boolean} values to populate to
   *                             indicate how the relationships had been patched
   *                             since the previous entity hash was stored.
   * @return The previous entity hash if updated, <code>null</code> if inserted,
   *         or empty-string if nothing was changed because the entity hashes
   *         were the same.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected String ensureEntityRow(Connection         conn,
                                   SzResolvedEntity   newEntity,
                                   Map<Long,Boolean>  prevRelationPatchMap)
    throws SQLException
  {
    PreparedStatement ps = null;
    ResultSet         rs = null;
    String operationId = this.generateOperationId(newEntity.getEntityId());

    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_entity AS t1 ("
              + " entity_id, entity_name, record_count, relation_count, "
              + " entity_hash, relations_hash, creator_id, modifier_id) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
              + "ON CONFLICT (entity_id) DO UPDATE SET"
              + " entity_name = EXCLUDED.entity_name,"
              + " record_count = EXCLUDED.record_count,"
              + " relation_count = EXCLUDED.relation_count,"
              + " entity_hash = EXCLUDED.entity_hash,"
              + " relations_hash = EXCLUDED.relations_hash,"
              + " prev_entity_hash = t1.entity_hash,"
              + " prev_relations_hash = t1.relations_hash,"
              + " modifier_id = EXCLUDED.modifier_id "
              + "WHERE t1.entity_hash <> EXCLUDED.entity_hash "
              + "OR t1.relations_hash IS NOT NULL"); // update if we have patches

      ps.setLong(1, newEntity.getEntityId());
      ps.setString(2, newEntity.getEntityName());
      ps.setInt(3, newEntity.getRecords().size());
      ps.setInt(4, newEntity.getRelatedEntities().size());
      ps.setString(5, newEntity.toHash());
      ps.setNull(6, Types.VARCHAR);
      ps.setString(7, operationId);
      ps.setString(8, operationId);

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
          "SELECT prev_relations_hash, prev_entity_hash "
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

      // get the previous relations hash
      String prevRelationsHash = rs.getString(1);
      if (rs.wasNull()) prevRelationsHash = null;
      this.parsePatchHash(prevRelationsHash, prevRelationPatchMap);

      // get the previous entity hash
      String prevEntityHash = rs.getString(2);
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
   * Attempts to insert the entity row.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param newEntity The {@link SzResolvedEntity} describing the entity.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void updateEntityRow(Connection       conn,
                                 SzResolvedEntity newEntity)
      throws SQLException
  {
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(
          "UPDATE sz_dm_entity SET"
              + " entity_name = ?,  record_count = ?,"
              + " relation_count = ?, entity_hash = ? "
              + "WHERE entity_id = ?");

      ps.setString(1, newEntity.getEntityName());
      ps.setInt(2, newEntity.getRecords().size());
      ps.setInt(3, newEntity.getRelatedEntities().size());
      ps.setString(4, newEntity.toHash());
      ps.setLong(5, newEntity.getEntityId());

      int rowCount = ps.executeUpdate();

      if (rowCount != 1) {
        throw new IllegalStateException(
            "Updated/Inserted an unexpected number of sz_dm_entity rows: "
                + rowCount);
      }

    } finally {
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
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   *
   * @return The number of records actually created.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int ensureAddedRecords(Connection   conn,
                                   EntityDelta  entityDelta,
                                   Scheduler    followUpScheduler)
    throws SQLException
  {
    Set<SzRecord> addedRecords = entityDelta.getAddedRecords();
    if (addedRecords.size() == 0) return 0;

    PreparedStatement ps            = null;
    ResultSet         rs            = null;
    String            operationId   = this.generateOperationId();
    int               createdCount  = 0;
    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_record ("
          + " data_source, record_id, entity_id, creator_id, modifier_id) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (data_source, record_id) DO UPDATE SET"
          + " entity_id = EXCLUDED.entity_id,"
          + " modifier_id = EXCLUDED.modifier_id");

      this.batchUpdate(ps, addedRecords, (ps2, record) -> {
        ps2.setString(1, record.getDataSource());
        ps2.setString(2, record.getRecordId());
        ps2.setLong(3, entityDelta.getEntityId());
        ps2.setString(4, operationId);
        ps2.setString(5, operationId);
        return 1;
      });

      ps = close(ps);

      // now get the ones that were actually added rather than updated
      ps = conn.prepareStatement(
          "SELECT data_source, record_id FROM sz_dm_record WHERE"
          + " entity_id = ? AND creator_id = ?");

      ps.setLong(1, entityDelta.getEntityId());
      ps.setString(2, operationId);

      rs = ps.executeQuery();
      while (rs.next()) {
        String    dataSource = rs.getString(1);
        String    recordId   = rs.getString(2);
        SzRecord  record     = new SzRecord(dataSource, recordId);

        // flag the record as created
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
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use to obtain the removed
   *                    records.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   * @return The number of records actually modified (i.e.: those that have
   *         not already been claimed by other entities).
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int orphanRemovedRecords(Connection   conn,
                                     EntityDelta  entityDelta,
                                     Scheduler    followUpScheduler)
      throws SQLException
  {
    Set<SzRecord> removedRecords = entityDelta.getRemovedRecords();
    if (removedRecords.size() == 0) return 0;

    PreparedStatement ps            = null;
    String            operationId   = this.generateOperationId();
    int               createdCount  = 0;
    try {
      ps = conn.prepareStatement(
          "UPDATE sz_dm_record entity_id = 0, modifier_id = ? "
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

      // iterate over the row counts
      int index = 0, modifiedCount = 0;
      for (SzRecord record: removedRecords) {
        int rowCount = rowCounts.get(index++);
        if (rowCount == 0) continue;
        modifiedCount++;

        // flag the record as deleted
        entityDelta.orphanedRecord(record);
      }

      return modifiedCount;

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Ensures the added and changed relationship rows exist in the database with
   * the correct match type and data source summaries.  Additionally, this will
   * track all rows that were actually inserted or updated using the {@link
   * EntityDelta#storedRelationship(long, long, SzMatchType, Map, Map)}
   * function on the specified {@link EntityDelta}.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   *
   * @return The number of relationships modified or created.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int ensureCurrentRelations(Connection   conn,
                                       EntityDelta  entityDelta,
                                       Scheduler    followUpScheduler)
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

      // check if the expected count is zero and shot-circuit here
      if (expectedCount == 0) return 0;

      // now get the ones that were actually added or modified along with their
      // previous relationship hashes
      ps = conn.prepareStatement(
          "SELECT relation_hash, prev_relation_hash "
              + "FROM sz_dm_relation WHERE modifier_id = ?");

      ps.setString(1, operationId);

      rs = ps.executeQuery();

      int updateCount = 0;
      while (rs.next()) {
        String relHash  = rs.getString(1);
        String prevHash = rs.getString(2);

        SzRelationship relationship = SzRelationship.parseHash(relHash);
        SzRelationship prevRelation = SzRelationship.parseHash(prevHash);

        long relatedId
            = (relationship.getEntityId() == entityDelta.getEntityId())
            ? relationship.getRelatedEntityId() : relationship.getEntityId();

        updateCount++;
        // check if previously, there was no relationship
        if (prevRelation == null) {
          entityDelta.storedRelationship(relationship.getEntityId(),
                                         relationship.getRelatedEntityId(),
                                         null,
                                         null,
                                         null);


          followUpScheduler.createTaskBuilder(REFRESH_RELATION.toString())
              .resource(ENTITY_RESOURCE_KEY, relatedId)
              .parameter(RefreshRelationHandler.ENTITY_ID_KEY, relatedId)
              .parameter(RefreshRelationHandler.RELATED_ENTITY_ID_KEY,
                         entityDelta.getEntityId());

        } else {
          entityDelta.storedRelationship(
              relationship.getEntityId(),
              relationship.getRelatedEntityId(),
              relationship.getMatchType(),
              prevRelation.getSourceSummary(),
              prevRelation.getRelatedSourceSummary());
        }
      }

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

      return updateCount;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Ensures the added and changed relationship rows exist in the database with
   * the correct match type and data source summaries.  Additionally, this will
   * track all rows that were actually inserted or updated using the {@link
   * EntityDelta#storedRelationship(long, long, SzMatchType, Map, Map)}
   * function on the specified {@link EntityDelta}.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityDelta The {@link EntityDelta} to use.
   * @param followUpScheduler The {@link Scheduler} to use for scheduling
   *                          follow-up tasks.
   *
   * @return The number of relationships modified or created.
   */
  protected int ensureRemovedRelations(Connection   conn,
                                       EntityDelta  entityDelta,
                                       Scheduler    followUpScheduler)
      throws SQLException
  {
    Map<Long, SzRelatedEntity> relations = entityDelta.getRemovedRelations();
    if (relations.size() == 0) return 0;
    System.err.println("REMOVING RELATIONSHIPS: " + relations);

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

      // check if the expected count is zero and shot-circuit here
      if (expectedCount == 0) return 0;

      // now get the ones that were actually added or modified along with their
      // current relationship hashes
      ps = conn.prepareStatement(
          "SELECT relation_hash "
              + "FROM sz_dm_relation WHERE modifier_id = ?");

      ps.setString(1, operationId);

      rs = ps.executeQuery();

      List<SzRelationship> pendingDelete = new ArrayList<>(expectedCount);

      while (rs.next()) {
        String hash = rs.getString(1);

        SzRelationship relationship = SzRelationship.parseHash(hash);
        if (relationship == null) {
          throw new IllegalStateException(
              "Existing relationship exists but has no hash.  relationship=[ "
                  + relationship + " ]");
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
          System.err.println(
              LoggingUtilities.multilineFormat(
                  "",
                  "--------------------------------------------------------------",
                  "WARNING: Relationship was deleted externally despite lock. ",
                  "This may cause reporting totals to be incorrect.",
                  "RELATIONSHIP : " + relationship));
          continue;
        }

        // increment the deleted count
        deletedCount++;

        long relatedId
            = (relationship.getEntityId() == entityDelta.getEntityId())
            ? relationship.getRelatedEntityId() : relationship.getEntityId();

        // mark the relationship as deleted
        entityDelta.deletedRelationship(relationship.getEntityId(),
                                        relationship.getRelatedEntityId(),
                                        relationship.getMatchType(),
                                        relationship.getSourceSummary(),
                                        relationship.getRelatedSourceSummary());

        followUpScheduler.createTaskBuilder(REFRESH_RELATION.toString())
            .resource(ENTITY_RESOURCE_KEY, relatedId)
            .parameter(RefreshRelationHandler.ENTITY_ID_KEY, relatedId)
            .parameter(RefreshRelationHandler.RELATED_ENTITY_ID_KEY,
                       entityDelta.getEntityId());
      }

      return deletedCount;

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
              + " entity_relation_delta, record_relation_delta,"
              + " entity_id, related_id) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?)");

      List<Integer> rowCounts = this.batchUpdate(
          ps, updates, (ps2, update) -> {

            ps2.setString(1, update.getReportKey().toString());
            ps2.setInt(2, update.getEntityDelta());
            ps2.setInt(3, update.getRecordDelta());
            ps2.setInt(4, update.getEntityRelationDelta());
            ps2.setInt(5, update.getRecordRelationDelta());
            ps2.setLong(6, update.getEntityId());

            Long relatedId = update.getRelatedEntityId();
            if (relatedId == null) {
              ps2.setNull(7, Types.INTEGER);
            } else {
              ps2.setLong(7, relatedId);
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
   * @throws SQLException If a failure occurs.
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
