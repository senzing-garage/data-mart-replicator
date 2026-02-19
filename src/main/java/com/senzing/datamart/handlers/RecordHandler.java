package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.*;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzNotFoundException;
import com.senzing.sql.SQLUtilities;

import javax.json.JsonObject;
import java.sql.*;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;
import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.listener.service.AbstractListenerService.*;
import static com.senzing.util.LoggingUtilities.*;

import static com.senzing.sdk.SzFlag.*;

/**
 * Provides a handler for scheduling a refresh on an entity based
 * on a record key.
 */
public class RecordHandler extends AbstractTaskHandler {
    /**
     * The parameter key for the data source code.
     */
    public static final String DATA_SOURCE_KEY = "DATA_SOURCE";

    /**
     * The parameter key for the record ID.
     */
    public static final String RECORD_ID_KEY = "RECORD_ID";

    /**
     * Constructs with the specified {@link SzReplicationProvider} 
     * to use to access the data mart replicator functions.
     *
     * @param provider The {@link SzReplicationProvider} to use.
     */
    public RecordHandler(SzReplicationProvider provider) {
        super(provider, HANDLE_RECORD);
    }

    /**
     * Implemented to handle the {@link 
     * com.senzing.datamart.SzReplicationProvider.TaskAction#HANDLE_RECORD} 
     * action by scheduling an entity refresh for a given record key.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected void handleTask(Map<String, Object>   parameters, 
                              int                   multiplicity,
                              Scheduler             followUpScheduler) 
        throws ServiceExecutionException 
    {
        logDebug("GOT HERE!!!!");
        Connection          conn    = null;
        PreparedStatement   ps      = null;
        ResultSet           rs      = null;
        try {
            // get the data source code and record ID
            String dataSource = ((String) parameters.get(DATA_SOURCE_KEY));
            String recordId = ((String) parameters.get(RECORD_ID_KEY));

            if (dataSource == null || dataSource.trim().length() == 0) {
                throw new IllegalArgumentException(
                    "Missing " + DATA_SOURCE_KEY + " parameter: " + parameters);
            }
            if (recordId == null || recordId.trim().length() == 0) {
                throw new IllegalArgumentException(
                    "Missing " + RECORD_ID_KEY + " parameter: " + parameters);
            }
            
            SzRecordKey recordKey = new SzRecordKey(dataSource, recordId);
            
            // get the environment
            SzEnvironment env = this.getSzEnvironment();
            SzEngine engine = env.getEngine();

            // get the entity
            String jsonText = null;
            try {
                jsonText = engine.getEntity(recordKey.toKey(), SZ_NO_FLAGS);

            } catch (SzNotFoundException e) {
                // let the result be null to indicate deletion
                jsonText = null;

            } catch (SzException e) {
                if (!NOT_FOUND_ERROR_CODES.contains(e.getErrorCode())) {
                    throw e;
                }
            }

            Long entityId = null;
            if (jsonText != null) {
                JsonObject jsonObj = parseJsonObject(jsonText);
                SzResolvedEntity entity = SzResolvedEntity.parse(jsonObj);
                entityId = entity.getEntityId();
                followUpOnEntity(followUpScheduler, recordKey, entityId);
            }

            // get the connection
            conn = this.getConnection();

            // prepare a query to check the data mart entity ID
            ps = conn.prepareStatement(
                "SELECT entity_id FROM sz_dm_record "
                + "WHERE data_source = ? AND record_id = ?");

            ps.setString(1, dataSource);
            ps.setString(2, recordId);

            rs = ps.executeQuery();

            if (rs.next()) {
                Long id = rs.getLong(1);

                if (!Objects.equals(entityId, id)) {
                    followUpOnEntity(followUpScheduler, recordKey, id);
                }
            }

            // release JDBC resources
            rs = close(rs);
            ps = close(ps);

            // commit the transaction -- this will release any locked rows
            conn.commit();

            // close the connection here
            conn = close(conn);

            // commit the follow-up scheduler
            followUpScheduler.commit();

        } catch (Exception e) {
            logError(e, "UNEXPECTED FAILURE -- ROLLING BACK TRANSACTION....");
            // rollback the transaction
            try {
                SQLUtilities.rollback(conn);

            } catch (Exception e2) {
                logError(e2, "FAILED TO ROLLBACK: ");
                System.err.println(e2.getMessage());
                System.err.println(formatStackTrace(e2.getStackTrace()));
            }
            throw new ServiceExecutionException(e);

        } finally {
            rs = close(rs);
            ps = close(ps);
            conn = close(conn);
        }
    }

    /**
     * Schedules a follow-up refresh on the specified entity using the specified
     * {@link Scheduler}.
     *
     * @param followUpScheduler The {@link Scheduler} to use for scheduling
     *                          follow-up tasks.
     * @param recordKey         The {@link SzRecordKey} of the record triggering
     *                          the follow-up.
     * @param entityId          The entity ID of the entity we are refreshing.
     */
    private static void followUpOnEntity(Scheduler      followUpScheduler,
                                         SzRecordKey    recordKey,
                                         long           entityId) 
    {
        logDebug("RECORD " + recordKey + " FOLLOWING UP ON ENTITY " + entityId);
        followUpScheduler.createTaskBuilder(REFRESH_ENTITY.toString())
                .resource(ENTITY_RESOURCE_KEY, entityId)
                .parameter(RefreshEntityHandler.ENTITY_ID_KEY, entityId)
                .schedule(true);
    }

}
