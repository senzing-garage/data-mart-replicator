package com.senzing.datamart.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides a data mart schema builder for SQLite.
 */
public class SQLiteSchemaBuilder extends SchemaBuilder {
  /**
   * Default constructor.
   */
  public SQLiteSchemaBuilder() {
    // do nothing
  }

  /**
   * Ensures the SQLite schema exists and optionally drops the schema
   * before creating it.
   *
   * @param conn The JDBC {@link Connection} to use for creating the schema.
   *
   * @param recreate <code>true</code> if the schema should be dropped and
   *                 recreated, or <code>false</code> if any existing schema
   *                 should be left in place.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  @Override
  public void ensureSchema(Connection conn, boolean recreate)
      throws SQLException
  {
    List<String> sqlList = new LinkedList<>();

    String createLockTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_locks ("
        + "  resource_key TEXT NOT NULL PRIMARY KEY, "
        + "  modifier_id TEXT NOT NULL);";

    String dropLockTable = "DROP TABLE IF EXISTS sz_dm_locks;";

    String createEntityTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_entity ("
        + "  entity_id INTEGER NOT NULL PRIMARY KEY, "
        + "  entity_name TEXT, "
        + "  record_count INTEGER, "
        + "  relation_count INTEGER, "
        + "  entity_hash TEXT, "
        + "  prev_entity_hash TEXT,"
        + "  creator_id TEXT NOT NULL, "
        + "  modifier_id TEXT NOT NULL, "
        + "  created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "  modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')))";

    String dropEntityTable = "DROP TABLE IF EXISTS sz_dm_entity;";

    String createEntityNewIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_entity_new_ix ON sz_dm_entity ("
        + "creator_id)";

    String createEntityModIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_entity_mod_ix ON sz_dm_entity ("
        + "modifier_id)";

    String dropEntityNewIndex = "DROP INDEX IF EXISTS sz_dm_entity_new_ix;";

    String dropEntityModIndex = "DROP INDEX IF EXISTS sz_dm_entity_mod_ix;";

    String createEntityInsertTrigger
        = formatCreateSQLiteInsertTrigger("sz_dm_entity");

    String dropEntityInsertTrigger
        = formatDropSQLiteInsertTrigger("sz_dm_entity");

    String createEntityUpdateTrigger
        = formatCreateSQLiteUpdateTrigger("sz_dm_entity");

    String dropEntityUpdateTrigger
        = formatDropSQLiteUpdateTrigger("sz_dm_entity");

    String createRecordTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_record ("
        + "  data_source TEXT NOT NULL, "
        + "  record_id TEXT NOT NULL, "
        + "  entity_id INTEGER NOT NULL, "
        + "  creator_id TEXT NOT NULL, "
        + "  modifier_id TEXT NOT NULL, "
        + "  adopter_id TEXT NULL, "
        + "  created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "  modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "PRIMARY KEY(data_source, record_id));";

    String dropRecordTable = "DROP TABLE IF EXISTS sz_dm_record;";

    String createRecordInsertTrigger
        = formatCreateSQLiteInsertTrigger("sz_dm_record");

    String dropRecordInsertTrigger
        = formatDropSQLiteInsertTrigger("sz_dm_record");

    String createRecordUpdateTrigger
        = formatCreateSQLiteUpdateTrigger("sz_dm_record");

    String dropRecordUpdateTrigger
        = formatDropSQLiteUpdateTrigger("sz_dm_record");

    String createRecordIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_record_ix ON sz_dm_record ("
        + "entity_id)";

    String dropRecordIndex
        = "DROP INDEX IF EXISTS sz_dm_record_ix;";

    String createRecordNewIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_record_new_ix ON sz_dm_record ("
        + "creator_id)";

    String dropRecordNewIndex
        = "DROP INDEX IF EXISTS sz_dm_record_new_ix;";

    String createRecordModIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_record_mod_ix ON sz_dm_record ("
        + "modifier_id)";

    String dropRecordModIndex
        = "DROP INDEX IF EXISTS sz_dm_record_mod_ix;";

    String createRelationTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_relation ("
        + "  entity_id INTEGER NOT NULL, "
        + "  related_id INTEGER NOT NULL, "
        + "  match_level INTEGER, "
        + "  match_key TEXT, "
        + "  match_type TEXT, "
        + "  relation_hash TEXT, "
        + "  prev_relation_hash TEXT, "
        + "  creator_id TEXT NOT NULL, "
        + "  modifier_id TEXT NOT NULL, "
        + "  created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "  modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "PRIMARY KEY(entity_id, related_id));";

    String dropRelationTable = "DROP TABLE IF EXISTS sz_dm_relation;";

    String createRelationIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_relation_ix ON sz_dm_relation ("
        + "related_id);";

    String dropRelationIndex = "DROP INDEX IF EXISTS sz_dm_relation_ix;";

    String createRelationNewIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_relation_new_ix ON sz_dm_relation ("
        + "creator_id);";

    String dropRelationNewIndex = "DROP INDEX IF EXISTS sz_dm_relation_new_ix;";

    String createRelationModIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_relation_mod_ix ON sz_dm_relation ("
        + "modifier_id);";

    String dropRelationModIndex = "DROP INDEX IF EXISTS sz_dm_relation_mod_ix;";

    String createRelationInsertTrigger
        = formatCreateSQLiteInsertTrigger("sz_dm_relation");

    String dropRelationInsertTrigger
        = formatDropSQLiteInsertTrigger("sz_dm_relation");

    String createRelationUpdateTrigger
        = formatCreateSQLiteUpdateTrigger("sz_dm_relation");

    String dropRelationUpdateTrigger
        = formatDropSQLiteUpdateTrigger("sz_dm_relation");

    String createReportTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_report ("
        + "  report_key TEXT NOT NULL PRIMARY KEY, "
        + "  report TEXT NOT NULL, "
        + "  statistic TEXT NOT NULL, "
        + "  data_source1 TEXT, "
        + "  data_source2 TEXT, "
        + "  entity_count INTEGER, "
        + "  record_count INTEGER, "
        + "  relation_count INTEGER, "
        + "  report_notes TEXT, "
        + "  created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "  modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')));";

    String dropReportTable = "DROP TABLE IF EXISTS sz_dm_report;";

    String createReportInsertTrigger
        = formatCreateSQLiteInsertTrigger("sz_dm_report");

    String dropReportInsertTrigger
        = formatDropSQLiteInsertTrigger("sz_dm_report");

    String createReportUpdateTrigger
        = formatCreateSQLiteUpdateTrigger("sz_dm_report");

    String dropReportUpdateTrigger
        = formatDropSQLiteUpdateTrigger("sz_dm_report");

    String createReportDetailTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_report_detail ("
        + "  report_key TEXT NOT NULL, "
        + "  entity_id INTEGER NOT NULL, "
        + "  related_id INTEGER NOT NULL DEFAULT (0), "
        + "  stat_count INTEGER DEFAULT (0), "
        + "  report_notes TEXT, "
        + "  creator_id TEXT NOT NULL, "
        + "  modifier_id TEXT NOT NULL, "
        + "  created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "  modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "PRIMARY KEY(entity_id, related_id, report_key));";

    String dropReportDetailTable = "DROP TABLE IF EXISTS sz_dm_report_detail;";

    String createReportDetailIndex1
        = "CREATE INDEX IF NOT EXISTS sz_dm_rpt_detail_ix "
        + "ON sz_dm_report_detail (report_key, entity_id);";

    String dropReportDetailIndex1
        = "DROP INDEX IF EXISTS sz_dm_rpt_detail_ix;";

    String createReportDetailIndex2
        = "CREATE UNIQUE INDEX IF NOT EXISTS sz_dm_rpt_detail_uix "
        + "ON sz_dm_report_detail (related_id, entity_id, report_key);";

    String dropReportDetailIndex2
        = "DROP INDEX IF EXISTS sz_dm_rpt_detail_uix;";

    String createReportDetailNewIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_rpt_det_new_ix "
        + "ON sz_dm_report_detail (creator_id);";

    String dropReportDetailNewIndex
        = "DROP INDEX IF EXISTS sz_dm_rpt_det_new_ix;";

    String createReportDetailModIndex
        = "CREATE INDEX IF NOT EXISTS sz_dm_rpt_det_mod_ix "
        + "ON sz_dm_report_detail (modifier_id);";

    String dropReportDetailModIndex
        = "DROP INDEX IF EXISTS sz_dm_rpt_det_mod_ix;";

    String createReportDetailInsertTrigger
        = formatCreateSQLiteInsertTrigger("sz_dm_report_detail");

    String dropReportDetailInsertTrigger
        = formatDropSQLiteInsertTrigger("sz_dm_report_detail");

    String createReportDetailUpdateTrigger
        = formatCreateSQLiteUpdateTrigger("sz_dm_report_detail");

    String dropReportDetailUpdateTrigger
        = formatDropSQLiteUpdateTrigger("sz_dm_report_detail");

    String createPendingReportTable
        = "CREATE TABLE IF NOT EXISTS sz_dm_pending_report ("
        + "  report_key TEXT NOT NULL, "
        + "  lease_id TEXT, "
        + "  expire_lease_at TIMESTAMP, "
        + "  entity_delta INTEGER, "
        + "  record_delta INTEGER, "
        + "  relation_delta INTEGER, "
        + "  entity_id INTEGER, "
        + "  related_id INTEGER, "
        + "  created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "  modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')));";

    String dropPendingReportTable
        = "DROP TABLE IF EXISTS sz_dm_pending_report;";

    String createPendingReportIndex1
        = "CREATE INDEX IF NOT EXISTS sz_dm_pend_rpt_ix1 "
        + "ON sz_dm_pending_report (report_key, lease_id, expire_lease_at);";

    String dropPendingReportIndex1
        = "DROP INDEX IF EXISTS sz_dm_pend_rpt_ix1;";

    String createPendingReportIndex2
        = "CREATE INDEX IF NOT EXISTS sz_dm_pend_rpt_ix2 "
        + "ON sz_dm_pending_report (expire_lease_at, lease_id);";

    String dropPendingReportIndex2
        = "DROP INDEX IF EXISTS sz_dm_pend_rpt_ix2;";

    String createPendingReportInsertTrigger
        = formatCreateSQLiteInsertTrigger("sz_dm_pending_report");

    String dropPendingReportInsertTrigger
        = formatDropSQLiteInsertTrigger("sz_dm_pending_report");

    String createPendingReportUpdateTrigger
        = formatCreateSQLiteUpdateTrigger("sz_dm_pending_report");

    String dropPendingReportUpdateTrigger
        = formatDropSQLiteUpdateTrigger("sz_dm_pending_report");

    if (recreate) {
      sqlList.add(dropLockTable);

      sqlList.add(dropEntityNewIndex);
      sqlList.add(dropEntityModIndex);
      sqlList.add(dropEntityUpdateTrigger);
      sqlList.add(dropEntityInsertTrigger);
      sqlList.add(dropEntityTable);

      sqlList.add(dropRecordModIndex);
      sqlList.add(dropRecordNewIndex);
      sqlList.add(dropRecordIndex);
      sqlList.add(dropRecordUpdateTrigger);
      sqlList.add(dropRecordInsertTrigger);
      sqlList.add(dropRecordTable);

      sqlList.add(dropRelationModIndex);
      sqlList.add(dropRelationNewIndex);
      sqlList.add(dropRelationIndex);
      sqlList.add(dropRelationUpdateTrigger);
      sqlList.add(dropRelationInsertTrigger);
      sqlList.add(dropRelationTable);

      sqlList.add(dropReportUpdateTrigger);
      sqlList.add(dropReportInsertTrigger);
      sqlList.add(dropReportTable);

      sqlList.add(dropReportDetailModIndex);
      sqlList.add(dropReportDetailNewIndex);
      sqlList.add(dropReportDetailIndex2);
      sqlList.add(dropReportDetailIndex1);
      sqlList.add(dropReportDetailUpdateTrigger);
      sqlList.add(dropReportDetailInsertTrigger);
      sqlList.add(dropReportDetailTable);

      sqlList.add(dropPendingReportIndex2);
      sqlList.add(dropPendingReportIndex1);
      sqlList.add(dropPendingReportUpdateTrigger);
      sqlList.add(dropPendingReportInsertTrigger);
      sqlList.add(dropPendingReportTable);
    }
    sqlList.add(createLockTable);
    sqlList.add(createEntityTable);
    sqlList.add(createEntityNewIndex);
    sqlList.add(createEntityModIndex);
    sqlList.add(createEntityInsertTrigger);
    sqlList.add(createEntityUpdateTrigger);

    sqlList.add(createRecordTable);
    sqlList.add(createRecordIndex);
    sqlList.add(createRecordNewIndex);
    sqlList.add(createRecordModIndex);
    sqlList.add(createRecordInsertTrigger);
    sqlList.add(createRecordUpdateTrigger);

    sqlList.add(createRelationTable);
    sqlList.add(createRelationIndex);
    sqlList.add(createRelationNewIndex);
    sqlList.add(createRelationModIndex);
    sqlList.add(createRelationInsertTrigger);
    sqlList.add(createRelationUpdateTrigger);

    sqlList.add(createReportTable);
    sqlList.add(createReportInsertTrigger);
    sqlList.add(createReportUpdateTrigger);

    sqlList.add(createReportDetailTable);
    sqlList.add(createReportDetailIndex1);
    sqlList.add(createReportDetailIndex2);
    sqlList.add(createReportDetailNewIndex);
    sqlList.add(createReportDetailModIndex);
    sqlList.add(createReportDetailInsertTrigger);
    sqlList.add(createReportDetailUpdateTrigger);

    sqlList.add(createPendingReportTable);
    sqlList.add(createPendingReportIndex1);
    sqlList.add(createPendingReportIndex2);
    sqlList.add(createPendingReportInsertTrigger);
    sqlList.add(createPendingReportUpdateTrigger);

    System.err.println("CREATING TABLES....");
    this.executeStatements(conn, sqlList);
    System.err.println("CREATED TABLES.");

    PreparedStatement ps 
    = conn.prepareStatement("SELECT COUNT(*) FROM sz_dm_pending_report");
    ResultSet rs = ps.executeQuery();
    int count = rs.getInt(1);
    System.out.println(" ************ sz_dm_pending_report table created: "
                        + count);
    rs.close();
    ps.close();
  }

  /**
   * Formats a SQLite create trigger statement for the timestamp maintenance
   * trigger for the specified table name.
   *
   * @param tableName The table name for the create trigger statement.
   * @return The create trigger statement.
   */
  protected String formatCreateSQLiteInsertTrigger(String tableName) {
    return "CREATE TRIGGER IF NOT EXISTS " + tableName + "_new "
        + "AFTER INSERT ON " + tableName + " FOR EACH ROW "
        + "BEGIN UPDATE " + tableName + " "
        + "SET created_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),"
        + " modified_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) "
        + "WHERE task_id = new.task_id; END;";
  }

  /**
   * Formats a SQLite drop trigger statement for the timestamp maintenance
   * trigger for the specified table name.
   *
   * @param tableName The table name for the drop trigger statement.
   * @return The drop trigger statement.
   */
  protected String formatDropSQLiteInsertTrigger(String tableName) {
    return "DROP TRIGGER IF EXISTS " + tableName + "_new;";
  }

  /**
   * Formats a SQLite create trigger statement for the timestamp maintenance
   * trigger for the specified table name.
   *
   * @param tableName The table name for the create trigger statement.
   * @return The create trigger statement.
   */
  protected String formatCreateSQLiteUpdateTrigger(String tableName) {
    return "CREATE TRIGGER IF NOT EXISTS " + tableName + "_mod "
        + "AFTER UPDATE ON " + tableName + " FOR EACH ROW "
        + "BEGIN UPDATE " + tableName + " "
        + "SET created_on = old.created_on, "
        + " modified_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) "
        + "WHERE task_id = old.task_id; END;";
  }

  /**
   * Formats a SQLite drop trigger statement for the timestamp maintenance
   * trigger for the specified table name.
   *
   * @param tableName The table name for the drop trigger statement.
   * @return The drop trigger statement.
   */
  protected String formatDropSQLiteUpdateTrigger(String tableName) {
    return "DROP TRIGGER IF EXISTS " + tableName + "_mod;";
  }
}
