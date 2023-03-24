package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.*;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.sql.DatabaseType;
import com.senzing.util.LoggingUtilities;

import java.sql.*;
import java.util.*;

import static com.senzing.datamart.SzReplicationProvider.TaskAction;
import static com.senzing.sql.SQLUtilities.close;
import static java.lang.Math.*;
import static java.util.Map.*;

/**
 * Provides a handler for refreshing an affected entity.
 */
public abstract class UpdateReportHandler extends AbstractTaskHandler {
  /**
   * The parameter key for the report key.
   */
  private static final String REPORT_KEY_KEY = "reportKey";

  /**
   * The maximum duration for maintaining a lease on the pending update rows.
   */
  private static final long LEASE_DURATION = 60000L;

  /**
   * Used for converting nanoseconds to milliseconds.
   */
  private static final long ONE_MILLION = 1000000L;

  /**
   * Constructs with the specified {@link SzReplicationProvider} to use to
   * access the data mart replicator functions and {@link TaskAction} indicating
   * the {@link TaskAction} that this handler supports.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   * @param supportedAction The supported {@link TaskAction} for this instance.
   */
  protected UpdateReportHandler(SzReplicationProvider provider,
                                TaskAction            supportedAction)
  {
    super(provider, supportedAction);
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

      // get the report key parameter
      String      reportKeyText = (String) parameters.get(REPORT_KEY_KEY);
      SzReportKey reportKey = SzReportKey.parse(reportKeyText);

      // generate a lease ID
      String leaseId = this.generateOperationId(reportKey);

      // get the start time to track the lease
      long startTime = System.nanoTime();

      // lease the rows for this report key
      List<SzReportUpdate> updates = this.leaseReportUpdates(conn,
                                                             reportKey,
                                                             leaseId);

      // commit the lease
      conn.commit();

      // check if no updates
      if (updates.size() == 0) return;

      // update the report statistic
      this.updateReportStatistic(conn, reportKey, leaseId, updates);

      // update the report details
      this.updateReportDetails(conn, reportKey, leaseId, updates);

      // delete the leased updates
      this.deleteLeasedReportUpdates(conn, reportKey, leaseId, updates);

      // get the duration
      long duration = (System.nanoTime() - startTime) / ONE_MILLION;
      if (duration > LEASE_DURATION) {
        throw new IllegalStateException(
            "Exceeded lease duration.  It is possible another process will "
                + "double-count the leased rows.  Rolling back.  duration=[ "
                + duration + " ], leaseDuration=[ " + LEASE_DURATION
                + " ], leaseId=[ " + leaseId + "], reportKey=[ " + reportKey
                + " ]");
      }

      // commit the transaction
      conn.commit();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("Rolling back transaction....");
      // rollback the transaction
      try {
        if (conn != null) conn.rollback();
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
   * Leases the rows for the specified {@link SzReportKey} from the pending
   * report update table so nobody else will attempt to use them.  This method
   * also expires any old leases against the same {@link SzReportKey} that have
   * exceeded twice their lease duration.  This returns a {@link List} of
   * {@link SzReportUpdate} instances describing the leased pending updates, or
   * an empty list if no updates are pending for the specified {@link
   * SzReportKey}.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param reportKey The {@link SzReportKey} for the report stat being updated.
   * @param leaseId The {@link String} lease ID to use.
   *
   * @return The {@link List} of {@link SzReportUpdate} instances describing the
   *         updates, or an empty list if no updates are pending.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected List<SzReportUpdate> leaseReportUpdates(Connection  conn,
                                                    SzReportKey reportKey,
                                                    String      leaseId)
      throws SQLException
  {
    PreparedStatement ps = null;
    ResultSet         rs = null;
    try {
      long          now         = System.currentTimeMillis();
      long          leaseExpire = now + (LEASE_DURATION * 2); // less aggressive
      Timestamp     expireTime  = new Timestamp(leaseExpire);
      DatabaseType  dbType      = this.getDatabaseType();

      // expire old leases
      ps = conn.prepareStatement(
          "UPDATE sz_dm_pending_report "
              + "SET lease_id = NULL, expire_lease_at = NULL "
              + "WHERE report_key = ? AND lease_id IS NOT NULL "
              + "AND expire_lease_at < " + dbType.getTimestampBindingSQL());

      // bind the parameters
      ps.setString(1, reportKey.toString());
      dbType.setTimestamp(ps, 2, expireTime);

      // update
      int rowCount = ps.executeUpdate();

      // if we expired a lease, then warn
      if (rowCount > 0) {
        System.err.println(
            LoggingUtilities.multilineFormat(
                "",
                "------------------------------------------------------------",
                "WARNING: Expired lease on rows (" + rowCount + ") for report "
                + "update. ",
                "REPORT KEY : " + reportKey.toString()));
      }

      // now lease the rows
      ps = conn.prepareStatement(
          "UPDATE sz_dm_pending_report SET"
              + " lease_id = ?,"
              + " expire_lease_at = " + dbType.getTimestampBindingSQL() + " "
              + "WHERE report_key = ? AND lease_id IS NULL AND"
              + " expire_lease_at IS NULL");

      // determine when the lease will expire
      now         = System.currentTimeMillis();
      leaseExpire = now + LEASE_DURATION;
      expireTime  = new Timestamp(leaseExpire);

      // bind the parameters
      ps.setString(1, leaseId);
      dbType.setTimestamp(ps, 2, expireTime);
      ps.setString(3, reportKey.toString());

      // determine how many rows were leased
      rowCount = ps.executeUpdate();

      if (rowCount == 0) return Collections.emptyList();
      ps = close(ps);

      // prepare the result
      List<SzReportUpdate> updates = new ArrayList<>(rowCount);

      // now get the updates
      ps = conn.prepareStatement(
          "SELECT"
              + " entity_delta, record_delta, relation_delta,"
              + " entity_id, related_id "
              + "FROM sz_dm_pending_report "
              + "WHERE report_key = ? AND lease_id = ?");

      ps.setString(1, reportKey.toString());
      ps.setString(2, leaseId);

      // execute the query
      rs = ps.executeQuery();

      // read the results
      while (rs.next()) {
        int   entityDelta           = rs.getInt(1);
        int   recordDelta           = rs.getInt(2);
        int   relationDelta         = rs.getInt(3);
        long  entityId              = rs.getLong(4);
        Long  relatedId             = rs.getLong(5);

        if (rs.wasNull()) relatedId = null;

        SzReportUpdate update
            = (relatedId == null)
            ? new SzReportUpdate(reportKey, entityId)
            : new SzReportUpdate(reportKey, entityId, relatedId);

        update.setEntityDelta(entityDelta);
        update.setRecordDelta(recordDelta);
        update.setRelationDelta(relationDelta);

        updates.add(update);
      }

      // close the result set and statement
      rs = close(rs);
      ps = close(ps);

      // check how many we got
      if (updates.size() != rowCount) {
        throw new IllegalStateException(
            "Failed to retrieve all leased rows.  expected=[ " + rowCount
                + " ], found=[ " + updates.size() + " ], updates=[ " + updates
            + " ]");
      }

      // return the updates
      return updates;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Totals the report deltas for each report statistic value and applies the
   * cumulative delta for each report statistic value for the report statistic
   * value being modified.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param reportKey The {@link SzReportKey} for the report stat being updated.
   * @param leaseId The {@link String} lease ID to use.
   * @param updates The {@link List} of {@link SzReportUpdate} instances
   *                describing the pending updates.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void updateReportStatistic(Connection           conn,
                                       SzReportKey          reportKey,
                                       String               leaseId,
                                       List<SzReportUpdate> updates)
    throws SQLException
  {
    PreparedStatement ps = null;
    try {
      int entityDelta           = 0;
      int recordDelta           = 0;
      int relationDelta         = 0;

      for (SzReportUpdate update: updates) {
        if (!reportKey.equals(update.getReportKey())) {
          throw new IllegalArgumentException(
              "At least one report update does not match the specified report "
              + "key.  reportKey=[ " + reportKey + " ], reportUpdate=[ "
              + update + " ]");
        }
        entityDelta           += update.getEntityDelta();
        recordDelta           += update.getRecordDelta();
        relationDelta         += update.getRelationDelta();
      }

      if (entityDelta == 0 && recordDelta == 0 && relationDelta == 0)
      {
        // short circuit early since the cumulative change being applied is no
        // change at all
        return;
      }

      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_report AS t1 ("
              + " report_key, report, statistic, data_source1, data_source2,"
              + " entity_count, record_count, relation_count ) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
              + "ON CONFLICT (report_key) DO UPDATE SET"
              + " entity_count = t1.entity_count + EXCLUDED.entity_count,"
              + " record_count = t1.record_count + EXCLUDED.record_count,"
              + " relation_count = t1.relation_count"
              + " + EXCLUDED.relation_count");

      ps.setString(1, reportKey.toString());
      ps.setString(2, reportKey.getReportCode().getCode());
      ps.setString(3, reportKey.getStatistic());
      ps.setString(4, reportKey.getDataSource1());
      ps.setString(5, reportKey.getDataSource2());
      ps.setInt(6, entityDelta);
      ps.setInt(7, recordDelta);
      ps.setInt(8, relationDelta);

      int rowCount = ps.executeUpdate();

      // check the row count
      if (rowCount != 1) {
        throw new IllegalStateException(
            "Expected exactly 1 row to be updated, but " + rowCount
            + "rows were updated.");
      }

      // free resources
      ps = close(ps);

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Updates the report details to match the pending updates.  This applies the
   * delta updates to the "stat count" column of each modified row and if the
   * value drops to zero (0) then deletes those rows.
   *
   * <b>NOTE:</b> Rows with negative stat count values should be considered
   * deleted and exist only to absorb any positive deltas that may later be
   * applied to restore them to zero (0) or a positive value.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param reportKey The {@link SzReportKey} for the report stat being updated.
   * @param leaseId The {@link String} lease ID to use.
   * @param updates The {@link List} of {@link SzReportUpdate} instances
   *                describing the pending updates.
   * @return The number of rows inserted/updated.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int updateReportDetails(Connection            conn,
                                    SzReportKey           reportKey,
                                    String                leaseId,
                                    List<SzReportUpdate>  updates)
      throws SQLException
  {
    PreparedStatement ps = null;
    try {
      // this map has text keys that are either entity ID's or encoded
      // relationship ID's
      Map<String, int[]> deltaSumMap = new LinkedHashMap<>();

      // split the updates into inserts and deletes by summing the deltas
      for (SzReportUpdate update: updates) {
        long entityId = update.getEntityId();
        Long relatedId = update.getRelatedEntityId();

        int entityDelta = update.getEntityDelta();
        int relationDelta = update.getRelationDelta();

        // check if we have an entity delta to record
        if (entityDelta != 0) {
          String key = String.valueOf(entityId);
          int[] deltaArr = deltaSumMap.get(key);
          if (deltaArr == null) {
            deltaArr = new int[]{0};
            deltaSumMap.put(key, deltaArr);
          }
          deltaArr[0] += entityDelta;
        }

        // check if we have a relation delta to record
        if (relationDelta != 0 && relatedId != null) {
          String key = min(entityId, relatedId) + ":"
              + max(entityId, relatedId);
          int[] deltaArr = deltaSumMap.get(key);
          if (deltaArr == null) {
            deltaArr = new int[]{0};
            deltaSumMap.put(key, deltaArr);
          }
          deltaArr[0] += relationDelta;
        }
      }

      // cleanup the delta sum map by removing any zero deltas
      Iterator<Entry<String,int[]>> iter = deltaSumMap.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String,int[]> entry = iter.next();
        if (entry.getValue()[0] == 0) iter.remove();
      }

      // handle the inserts / updates
      if (deltaSumMap.size() == 0) return 0;

      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_report_detail AS t1 ("
              + " report_key, entity_id, related_id, stat_count,"
              + " creator_id, modifier_id ) "
              + "VALUES (?, ?, ?, ?, ?, ?) "
              + "ON CONFLICT (report_key, entity_id, related_id) "
              + "DO UPDATE SET"
              + " stat_count = t1.stat_count + EXCLUDED.stat_count,"
              + " modifier_id = EXCLUDED.modifier_id");

      List<Integer> rowCounts = this.batchUpdate(
          ps, deltaSumMap.entrySet(), (ps2, entry) -> {

            String[] keyTokens = entry.getKey().split(":");
            int delta = entry.getValue()[0];

            long entityId = Long.parseLong(keyTokens[0]);
            Long relatedId = (keyTokens.length == 1) ? null
                : Long.parseLong(keyTokens[1]);

            ps2.setString(1, reportKey.toString());
            ps2.setLong(2, entityId);
            if (relatedId == null) {
              ps2.setLong(3, 0L);
            } else {
              ps2.setLong(3, relatedId);
            }
            ps2.setLong(4, delta);
            ps2.setString(5, leaseId);
            ps2.setString(6, leaseId);

            return 1;
          });

      // close the statement
      ps = close(ps);

      // verify the row counts
      int updateCount = sum(rowCounts);
      if (updateCount != deltaSumMap.size()) {
        throw new IllegalStateException(
            "Inserted/updated an unexpected number of detail rows.  "
                + "expected=[ " + deltaSumMap.size() + " ], actual=[ "
                + updateCount + " ], reportKey=[ " + reportKey + " ]");
      }

      // now delete any rows that dropped to a zero (0) count
      ps = conn.prepareStatement(
          "DELETE FROM sz_dm_report_detail "
              + "WHERE report_key = ? AND modifier_id = ? "
              + "AND stat_count = 0");

      ps.setString(1, reportKey.toString());
      ps.setString(2, leaseId);

      ps.executeUpdate();

      ps = close(ps);

      // return the number of rows inserted/updated/deleted
      return updateCount;

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Deletes the pending report updates that were leased and applied to the
   * report statistic.
   *
   * @param conn The JDBC {@link Connection} to use.
   * @param reportKey The {@link SzReportKey} for the report stat being updated.
   * @param leaseId The {@link String} lease ID to use.
   * @param updates The {@link List} of {@link SzReportUpdate} instances
   *                describing the pending updates.
   * @return The number of rows deleted.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int deleteLeasedReportUpdates(Connection            conn,
                                          SzReportKey           reportKey,
                                          String                leaseId,
                                          List<SzReportUpdate>  updates)
    throws SQLException
  {
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(
          "DELETE FROM sz_dm_pending_report "
              + "WHERE report_key = ? AND lease_id = ?");

      ps.setString(1, reportKey.toString());
      ps.setString(2, leaseId);

      int deleteCount = ps.executeUpdate();

      if (deleteCount != updates.size()) {
        throw new IllegalStateException(
            "Deleted an unexpected number of pending report update rows.  "
                + "expected=[ " + updates.size() + " ], deleted=[ "
                + deleteCount + " ], reportKey=[ " + reportKey
                + " ], leaseId=[ " + leaseId + " ]");
      }

      ps = close(ps);

      return deleteCount;

    } finally {
      ps = close(ps);
    }
  }
}
