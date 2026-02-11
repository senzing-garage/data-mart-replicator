package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.*;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.util.JsonUtilities;

import javax.json.JsonObject;
import java.sql.*;
import java.util.*;

import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzNotFoundException;

import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.datamart.model.SzReportStatistic.*;
import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.listener.service.AbstractListenerService.*;
import static com.senzing.sdk.SzFlag.*;

/**
 * Handles updates to the data source summary (DSS) report statistics.
 *
 * @see SzReportCode#DATA_SOURCE_SUMMARY
 */
public class SourceSummaryReportHandler extends UpdateReportHandler {
    /**
     * The flags to use when retrieving the entity from the G2 repository.
     */
    private static final Set<SzFlag> ENTITY_FLAGS = SZ_NO_FLAGS;

    /**
     * The error code when a record is encountered by Senzing SDK but it has no resolved
     * entity.  This is likely an indication of a race condition where the record no 
     * longer exists -- treat it as such.
     */
    private static final int SZ_ERR_RECORD_HAS_NO_RESOLVED_ENTITY = 38;

    /**
     * The error code when a record is encountered by Senzing SDK but it has no obs ent.
     * This is likely an indication of a race condition where the record no longer exists
     * -- treat it as such.
     */
    private static final int SZ_ERR_NO_OBSERVED_ENTITY_FOR_DSRC_ENTITY_KEY = 39;
    
    /**
     * The Senzing SDK error codes to treat as an {@link SzNotFoundException}.
     */
    private static final Set<Integer> NOT_FOUND_ERROR_CODES
        = Set.of(SZ_ERR_RECORD_HAS_NO_RESOLVED_ENTITY, 
                 SZ_ERR_NO_OBSERVED_ENTITY_FOR_DSRC_ENTITY_KEY);
    /**
     * Constructs with the specified {@link SzReplicationProvider}. This
     * constructs the super class with {@link
     * com.senzing.datamart.SzReplicationProvider.TaskAction#UPDATE_CROSS_SOURCE_SUMMARY} as the supported action.
     *
     * @param provider The {@link SzReplicationProvider} to use.
     */
    public SourceSummaryReportHandler(SzReplicationProvider provider) {
        super(provider, UPDATE_DATA_SOURCE_SUMMARY);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to special-case the record delta for the {@link
     * SzReportStatistic#ENTITY_COUNT} statistic so that only positive deltas from
     * the pending updates are considered and the negative values are pulled
     * directly from the record table where the entity ID is set to zero (0).
     */
    @Override
    protected int overrideRecordDelta(Connection            conn,
                                      SzReportKey           reportKey,
                                      List<SzReportUpdate>  updates,
                                      int                   computedSum,
                                      Scheduler             followUpScheduler)
        throws SQLException, SzException
    {
        // check if not ENTITY_COUNT statistic
        if (!ENTITY_COUNT.toString().equals(reportKey.getStatistic())) {
            return computedSum;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            int recordDelta = 0;

            for (SzReportUpdate update : updates) {
                if (update.getRecordDelta() < 0) {
                    continue;
                }
                recordDelta += update.getRecordDelta();
            }

            // get the data source
            String dataSource = reportKey.getDataSource1();

            // generate an operation ID
            String operationId = this.generateOperationId();

            // now lease the rows with no entity ID
            ps = conn.prepareStatement("UPDATE sz_dm_record SET modifier_id = ? "
                    + "WHERE data_source = ? AND entity_id = 0");

            ps.setString(1, operationId);
            ps.setString(2, dataSource);

            // execute the lease
            int leasedCount = ps.executeUpdate();

            ps = close(ps);

            if (leasedCount == 0) {
                logInfo("No rows leased for entity ID zero and "
                        + "data source: " + dataSource);
            }

            // select back the leased rows
            ps = conn.prepareStatement(
                "SELECT record_id FROM sz_dm_record "
                    + "WHERE entity_id = 0 AND data_source = ? AND modifier_id = ?");

            ps.setString(1, dataSource);
            ps.setString(2, operationId);

            rs = ps.executeQuery();

            // get the SzEnvironment
            SzEnvironment env = this.getSzEnvironment();
            SzEngine engine = env.getEngine();

            Set<SzRecordKey> deleteSet = new LinkedHashSet<>();
            Map<SzRecordKey, Long> reconnectMap = new LinkedHashMap<>();
            while (rs.next()) {
                String recordId = rs.getString(1);
                SzRecordKey recordKey = new SzRecordKey(dataSource, recordId);

                Long entityId = null;
                String jsonText = null;
                try {
                    jsonText = engine.getEntity(recordKey.toKey(), ENTITY_FLAGS);

                } catch (SzNotFoundException e) {
                    // do nothing and fall through
                
                } catch (SzException e) {
                    if (!NOT_FOUND_ERROR_CODES.contains(e.getErrorCode())) {
                        logWarning(e, "FAILED TO CHECK IF RECORD STILL EXISTS: " + recordKey);
                        continue;
                    }
                }

                if (jsonText != null) {
                    JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);

                    // dereference the resolved entity
                    if (jsonObject.containsKey("RESOLVED_ENTITY")) {
                        jsonObject = jsonObject.getJsonObject("RESOLVED_ENTITY");
                    }

                    // get the entity ID
                    entityId = JsonUtilities.getLong(jsonObject, "ENTITY_ID");

                    if (entityId == null) {
                        logWarning("Skipping orphan record + " + recordKey
                            + " due to missing entity ID in entity JSON: "
                            + jsonText);
                        continue;
                    }

                    // check the entity ID
                    ResultSet rs2 = null;
                    PreparedStatement ps2 = conn.prepareStatement(
                        "SELECT COUNT(*) FROM sz_dm_entity WHERE entity_id = ?");
                    ps2.setLong(1, entityId);
                    rs2 = ps2.executeQuery();
                    rs2.next();
                    int entityCount = rs2.getInt(1);
                    rs2 = close(rs2);
                    ps2 = close(ps2);
                    if (entityCount == 0) {
                        logDebug("Entity " + entityId + " for orphan record "
                            + recordKey + " has not yet been replicated.  "
                            + "Scheduling follow-up....");

                        followUpScheduler.createTaskBuilder(REFRESH_ENTITY.toString())
                            .resource(ENTITY_RESOURCE_KEY, entityId)
                            .parameter(RefreshEntityHandler.ENTITY_ID_KEY, entityId)
                            .schedule(true);

                        continue;

                    } else if (entityCount > 1) {
                        logWarning("Entity " + entityId + " for orphan record "
                            + recordKey + " has " + entityCount
                            + " data-mart rows.");
                        continue;
                    }
                }
                if (entityId == null) {
                    logDebug("Determined that record is truly deleted: " + recordKey);
                    deleteSet.add(new SzRecordKey(dataSource, recordId));

                } else {
                    logDebug("Queueing record " + recordKey
                        + " for reconnection to entity " + entityId);
                    reconnectMap.put(recordKey, entityId);
                }
            }

            rs = close(rs);
            ps = close(ps);

            // check if we have any to reconnect
            if (reconnectMap.size() > 0) {
                // reconnect the records that have been mistakenly orphaned
                ps = conn.prepareStatement(
                    "UPDATE sz_dm_record SET entity_id = ?, adopter_id = ? "
                    + "WHERE data_source = ? AND record_id = ? AND entity_id = 0 "
                    + "AND modifier_id = ?");

                List<Integer> rowCounts = this.batchUpdate(
                    ps, reconnectMap.entrySet(), (ps2, entry) -> 
                    {
                        SzRecordKey recordKey = entry.getKey();
                        Long entityId = entry.getValue();
                        ps2.setLong(1, entityId);
                        ps2.setString(2, operationId);
                        ps2.setString(3, recordKey.getDataSource());
                        ps2.setString(4, recordKey.getRecordId());
                        ps2.setString(5, operationId);
                        return -1;
                    });

                int index = 0;
                int reconnectedCount = 0;
                for (Map.Entry<SzRecordKey, Long> entry : reconnectMap.entrySet()) {
                    int rowCount = rowCounts.get(index++);
                    if (rowCount == 0) {
                        logWarning("FAILED TO RECONNECT RECORD " + entry.getKey()
                            + " TO ENTITY " + entry.getValue());
                    } else {
                        logDebug("Reconnected record " + entry.getKey()
                            + " to entity " + entry.getValue());
                        reconnectedCount++;
                    }
                }
                logDebug("Reconnected " + reconnectedCount + " out of "
                    + reconnectMap.size() + " records from " + dataSource
                    + " data source");
            }

            rs = close(rs);
            ps = close(ps);

            if (deleteSet.size() > 0) {
                // delete the leased rows
                ps = conn.prepareStatement("DELETE FROM sz_dm_record "
                        + "WHERE data_source = ? AND record_id = ? "
                        + "AND entity_id = 0 AND modifier_id = ?");

                List<Integer> rowCounts = this.batchUpdate(ps, deleteSet, (ps2, rec) -> {
                    ps2.setString(1, rec.getDataSource());
                    ps2.setString(2, rec.getRecordId());
                    ps2.setString(3, operationId);
                    return -1;
                });

                int index = 0;
                int deletedCount = 0;
                for (SzRecordKey recordKey : deleteSet) {
                    int rowCount = rowCounts.get(index++);
                    if (rowCount == 0) {
                        throw new IllegalStateException(
                            "Failed to delete leased orphan record row: "
                            + recordKey);
                    }
                    deletedCount++;
                }

                // the row count is the number to decrement by
                recordDelta -= deletedCount;
            }

            // release resources
            ps = close(ps);

            // return the record delta
            return recordDelta;

        } finally {
            ps = close(ps);
            rs = close(rs);
        }
    }
}
