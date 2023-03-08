package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.SzReportCode;
import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.model.SzReportUpdate;
import com.senzing.datamart.model.SzReportStatistic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static com.senzing.datamart.SzReplicationProvider.TaskAction;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.model.SzReportStatistic.*;
import static com.senzing.sql.SQLUtilities.close;

/**
 * Handles updates to the data source summary (DSS) report statistics.
 *
 * @see SzReportCode#DATA_SOURCE_SUMMARY
 */
public class SourceSummaryReportHandler extends UpdateReportHandler {

  /**
   * Constructs with the specified {@link SzReplicationProvider}.  This
   * constructs the super class with {@link
   * TaskAction#UPDATE_CROSS_SOURCE_SUMMARY} as the supported action.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   */
  public SourceSummaryReportHandler(SzReplicationProvider provider) {
    super(provider, UPDATE_DATA_SOURCE_SUMMARY);
  }

  /**
   * Overridden to special-case the {@link SzReportStatistic#RECORD_COUNT}
   * statistic so that only positive deltas from the pending updates are
   * considered and the negative values are pulled directly from the
   * record table where the entity ID is set to zero (0).
   *
   * {@inheritDoc}
   */
  protected void updateReportStatistic(Connection           conn,
                                       SzReportKey          reportKey,
                                       String               leaseId,
                                       List<SzReportUpdate> updates)
      throws SQLException
  {
    // check if not
    if (!RECORD_COUNT.toString().equals(reportKey.getStatistic())) {
      super.updateReportStatistic(conn, reportKey, leaseId, updates);
      return;
    }

    PreparedStatement ps = null;
    try {
      int recordDelta = 0;

      for (SzReportUpdate update: updates) {
        if (update.getRecordDelta() < 0) continue;
        recordDelta += update.getRecordDelta();
      }

      // get the data source
      String dataSource = reportKey.getDataSource1();

      // now lease the rows with no entity ID
      ps = conn.prepareStatement(
          "DELETE FROM sz_dm_record "
              + "WHERE data_source = ? AND entity_id = 0");

      ps.setString(1, dataSource);

      int rowCount = ps.executeUpdate();

      ps = close(ps);

      // the row count is the number to decrement by
      recordDelta -= rowCount;

      // check the record delta and see if there is nothing to update
      if (recordDelta == 0) return;

      // prepare the statement
      ps = conn.prepareStatement(
          "INSERT INTO sz_dm_report AS t1 ("
              + " report_key, report, statistic, data_source1, data_source2,"
              + " entity_count, record_count, entity_relation_count,"
              + " record_relation_count ) "
              + "VALUES (?, ?, ?, ?, ?, 0, ?, 0, 0) "
              + "ON CONFLICT (report_key) DO UPDATE SET"
              + " entity_count = EXCLUDED.entity_count,"
              + " record_count = t1.record_count + EXCLUDED.record_count,"
              + " entity_relation_count = EXCLUDED.entity_relation_count,"
              + " record_relation_count = EXCLUDED.record_relation_count");

      ps.setString(1, reportKey.toString());
      ps.setString(2, DATA_SOURCE_SUMMARY.getCode());
      ps.setString(3, RECORD_COUNT.toString());
      ps.setString(4, dataSource);
      ps.setString(5, dataSource);
      ps.setInt(6, recordDelta);

      rowCount = ps.executeUpdate();

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

}
