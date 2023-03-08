package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.model.SzResolvedEntity;
import com.senzing.datamart.model.SzReportCode;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.g2.G2Service;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.util.JsonUtilities;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.datamart.model.SzReportCode.*;

/**
 * Provides a handler for updating the relationship count of an entity.
 */
public class RefreshRelationHandler extends AbstractTaskHandler {
  /**
   * The parameter key for the entity ID of the entity being updated.
   */
  public static final String ENTITY_ID_KEY = "entityId";

  /**
   * The parameter key for the related entity ID.
   */
  public static final String RELATED_ENTITY_ID_KEY = "relatedEntityId";

  /**
   * The constant uses for the max-degrees parameter.
   */
  private static final int ONE_DEGREE = 1;

  /**
   * The flags to use when retrieving the entity from the G2 repository.
   */
  private static final long ENTITY_FLAGS
      = G2_ENTITY_INCLUDE_RECORD_DATA;

  /**
   * Constructs with the specified {@link SzReplicationProvider} to use to
   * access the data mart replicator functions.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   */
  public RefreshRelationHandler(SzReplicationProvider provider) {
    super(provider, REFRESH_RELATION);
  }

  /**
   * Implemented to verify the relationship to the specified entity has either
   * been added or deleted and then checks if it has already been accounted for
   * and if not increments or decrements the relationship count.
   *
   * @param parameters The {@link Map} of {@link String} keys to {@link Object}
   *                   values for the task parameters.
   * @param multiplicity The multiplicity for the task.
   * @param followUpScheduler The follow-up scheduler for the task.
   * @throws ServiceExecutionException
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
      long relatedId = ((Number)
          parameters.get(RELATED_ENTITY_ID_KEY)).longValue();

      // get the G2Service
      G2Service g2Service = this.getG2Service();

      // create the relationship set to populate
      Set<Long>           relatedEntityIds  = new LinkedHashSet<>();
      Map<Long, Boolean>  relPatchMap       = new LinkedHashMap<>();

      // ensure the row and get the previous entity hash
      String operationId = this.findCurrentRelationships(
          conn, entityId, relatedEntityIds, relPatchMap);

      // check if the entity does not yet exist
      if (operationId == null) {
        // assume we are going to refresh this entity soon since it is expected
        return;
      }

      // initialize the patch value -- true for a relationship, false for no
      // relationship and null for no action
      Boolean patch = null;

      // check whether the specified relationship exists in the G2 repository
      String jsonText = g2Service.findEntityPath(
          entityId, relatedId, ONE_DEGREE, ENTITY_FLAGS);

      // check if one of the entities does not exist -- this may be why the
      // relationship between them has been altered (i.e.: removed)
      if (jsonText == null) {
        // the JSON text is null so at least one of the entities was not found.
        // check if this entity exists (the related one may have been removed)
        jsonText = g2Service.getEntity(entityId, ENTITY_FLAGS);
        if (jsonText == null) {
          // this entity was deleted so simply stop here since it should get
          // a REFRESH_ENTITY update soon since it was an "affected entity"
          return;
        }

        // if we got here, then assume the other entity was the one deleted and
        // treat this like a deleted relationship (because the related is gone)
        patch = Boolean.FALSE; // false indicates no relationship

      } else {
        // if we get here then both entities exist, parse the JSON to check for
        // a path between them in the result
        JsonObject jsonObj = JsonUtilities.parseJsonObject(jsonText);

        // now check if related
        patch = this.checkRelated(jsonObj);
      }

      // check if the relationship patch is already accounted for
      if (relatedEntityIds.contains(relatedId) == patch.booleanValue()) {
        // the related entity is present or absent as expected by the patch
        return;
      }

      // if we get here, then we need to patch the relationships for this entity
      Boolean currentPatch = relPatchMap.get(relatedId);
      if (currentPatch == null) {
        // relation has never been patched, it is either being removed or added
        relPatchMap.put(relatedId, patch);

      } else {
        // current relationship state is governed by a prior patch and now we
        // are applying the opposite patch, so simply remove the patch
        relPatchMap.remove(relatedId);

        // do a sanity check here
        if (patch.booleanValue() != !currentPatch.booleanValue()) {
          throw new IllegalStateException(
              "Removing old patch but new patch is not the opposite.  "
                  + "entityId=[ " + entityId + " ], relatedId=[ " + relatedId
                  + " ], oldPatch=[ " + currentPatch + " ], newPatch=[ "
                  + patch + " ]");
        }
      }

      // get the old and new related count
      int oldRelatedCount = relatedEntityIds.size();
      int newRelatedCount = oldRelatedCount + (patch.booleanValue() ? 1 : -1);

      // update the relationship count and the relationship patch
      this.updateRelationCount(
          conn, entityId, newRelatedCount, relPatchMap, operationId);

      // insert into the pending report table and schedule the report update
      this.scheduleReportUpdates(
          conn, entityId, oldRelatedCount, newRelatedCount);

      // commit the transaction
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
   * Checks if the entity exists and if found populates the specified
   * {@link Set} with the related entity ID's of the relationships that have
   * been accounted for since the last refresh (including any relationship
   * patches).
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityId The entity ID of the entity to check for.
   * @param relatedEntityIds The {@link Set} of {@link Long} related entity ID's
   *                         to be populated (typically an empty set).
   *
   * @return The {@link String} operation ID that was set as the modifier ID
   *         to lock the entity row, or <code>null</code> if the entity was
   *         not found.
   */
  protected String findCurrentRelationships(
      Connection          conn,
      long                entityId,
      Set<Long>           relatedEntityIds,
      Map<Long, Boolean>  relationPatchMap)
    throws SQLException
  {
    PreparedStatement ps          = null;
    ResultSet         rs          = null;
    String            operationId = this.generateOperationId(entityId);
    try {
      ps = conn.prepareStatement(
          "UPDATE sz_dm_entity SET modifier_id = ? WHERE entity_id = ?");

      ps.setLong(1, entityId);

      // update the row with a new modifier ID
      int rowCount = ps.executeUpdate();

      ps = close(ps);

      // check if the row does not exist
      if (rowCount == 0) return null;

      // get the current entity and relationship hash
      ps = conn.prepareStatement(
          "SELECT entity_hash, patch_hash FROM sz_dm_entity "
          + "WHERE entity_id = ? AND modifier_id = ?");

      ps.setLong(1, entityId);
      ps.setString(2, operationId);

      // execute the query
      rs = ps.executeQuery();

      if (!rs.next()) {
        throw new IllegalStateException(
            "Failed to retrieve the entity row after locking it.  entityId=[ "
            + entityId + " ], modifierId=[ " + operationId + " ]");
      }

      // get the column values
      String entityHash = rs.getString(1);
      if (rs.wasNull()) entityHash = null;

      String relationHash = rs.getString(2);
      if (rs.wasNull()) relationHash = null;

      // parse the entity hash
      SzResolvedEntity entity = SzResolvedEntity.parseHash(entityHash);

      // close JDBC resources
      rs = close(rs);
      ps = close(ps);

      // get the related entities
      Set<Long> related = entity.getRelatedEntities().keySet();

      // check if no patches
      if (relationHash == null) {
        relatedEntityIds.addAll(related);
        return operationId;
      }

      // parse the hashes
      this.parsePatchHash(relationHash, relationPatchMap);

      // now iterate over the entities to populate the specified set
      related.forEach(relatedId -> {
        // check if the related entity ID is patched
        if (!relationPatchMap.containsKey(relatedId)) {
          // not patched so add it to the set
          relatedEntityIds.add(relatedId);
        } else {
          // get the patch
          boolean patch = relationPatchMap.get(relatedId).booleanValue();

          // only add it if not removed
          if (patch) {
            relatedEntityIds.add(relatedId);
          }
        }
      });

      // return true to indicate the entity was found
      return operationId;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Parses the entity path JSON described in the specified {@link JsonObject}
   * to determine if the two entities are related or not.
   *
   * @param jsonObject The {@link JsonObject} describing the entity path.
   *
   * @return <code>true</code> if related, otherwise <code>false</code>.
   */
  protected boolean checkRelated(JsonObject jsonObject) {
    JsonArray pathArray = jsonObject.getJsonArray("ENTITY_PATHS");
    if (pathArray.size() == 0) return false;

    JsonObject pathObject = pathArray.get(0).asJsonObject();

    Long startId = JsonUtilities.getLong(pathObject, "START_ENTITY_ID");
    Long endId = JsonUtilities.getLong(pathObject, "END_ENTITY_ID");
    JsonArray entities = pathObject.getJsonArray("ENTITIES");
    int count = entities.size();

    List<Long> list = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      list.add(entities.getJsonNumber(index).longValue());
    }

    return (list.size() == 2
            && startId.equals(list.get(0)) && endId.equals(list.get(1)));
  }

  /**
   * Updates the relationship count and patch hash for the specified entity.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityId The entity ID of the entity to update.
   * @param newRelatedCount The new relationship count for the entity.
   * @param relationPatchMap The {@link Map} of {@link Long} entity ID keys to
   *                         {@link Boolean} values describing the new
   *                         relationship patches.
   * @param operationId The {@link String} operation ID used when locking the
   *                    database row.
   */
  protected void updateRelationCount(Connection         conn,
                                     long               entityId,
                                     int                newRelatedCount,
                                     Map<Long,Boolean>  relationPatchMap,
                                     String             operationId)
    throws SQLException
  {
    PreparedStatement ps = null;
    try {
      String patchHash = formatPatchHash(relationPatchMap);

      ps = conn.prepareStatement(
          "UPDATE sz_dm_entity SET"
          + " relation_count = ?, patch_hash = ?, prev_patch_hash = patch_hash "
          + "WHERE entity_id = ? AND modifier_id = ?");

      ps.setInt(1, newRelatedCount);
      ps.setString(2, patchHash);
      ps.setLong(3, entityId);
      ps.setString(4, operationId);

      int rowCount = ps.executeUpdate();

      // check the row count
      if (rowCount != 1) {
        throw new IllegalStateException(
            "Unexpected number of entity rows updated for relationship patch.  "
            + "entityId=[ " + entityId + " ], modifierId=[ " + operationId
            + " ], relatedCount=[ " + newRelatedCount + " ], patchHash=[ "
            + patchHash + " ]");
      }

      ps = close(ps);

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Inserts the pending report udpates regarding the {@linkplain
   * SzReportCode#ENTITY_RELATION_BREAKDOWN entity relation breakdown} report
   * so that the statistic for the old relation count is decremented and the one
   * for the new relation count is incremented.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param entityId The entity ID of the entity being updated.
   * @param oldRelatedCount The old number of relationships for the entity.
   * @param newRelatedCount The new number of relationships for the entity.
   */
  protected void scheduleReportUpdates(Connection conn,
                                       long       entityId,
                                       int        oldRelatedCount,
                                       int        newRelatedCount)
    throws SQLException
  {
    PreparedStatement ps = null;
    try {
      SzReportKey oldReportKey = new SzReportKey(
          ENTITY_RELATION_BREAKDOWN, String.valueOf(oldRelatedCount));
      SzReportKey newReportKey = new SzReportKey(
          ENTITY_RELATION_BREAKDOWN, String.valueOf(newRelatedCount));

      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_pending_report"
          + " (report_key, entity_delta, entity_id) "
          + "VALUES (?, ?, ?)");

      ps.setString(1, oldReportKey.toString());
      ps.setInt(2, -1);
      ps.setLong(3, entityId);
      ps.addBatch();

      ps.setString(1, newReportKey.toString());
      ps.setInt(2, 1);
      ps.setLong(3, entityId);
      ps.addBatch();

      int[] rowCounts = ps.executeBatch();

      // cleanup
      ps = close(ps);

      // check the row counts
      if (rowCounts.length != 2 || rowCounts[0] != 1 || rowCounts[1] != 1) {
        throw new IllegalStateException(
            "Unexpected batch row counts.  rowCounts=[ " + rowCounts.length
            + " ], rowCount0=[ " + rowCounts[0] + " ], rowCount1=[ "
            + rowCounts[1] + " ]");
      }

      // schedule the report updates
      this.scheduleReportFollowUp(UPDATE_ENTITY_RELATION_BREAKDOWN,
                                  oldReportKey);
      this.scheduleReportFollowUp(UPDATE_ENTITY_RELATION_BREAKDOWN,
                                  newReportKey);

    } finally {
      ps = close(ps);
    }
  }
}
