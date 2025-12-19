package com.senzing.datamart.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides a data mart schema builder for PostgreSQL.
 */
public class PostgreSQLSchemaBuilder extends SchemaBuilder {
    /**
     * Default constructor.
     */
    public PostgreSQLSchemaBuilder() {
        // do nothing
    }

    /**
     * Ensures the PostgreSQL schema exists and optionally drops the schema
     * before creating it.
     *
     * @param conn     The JDBC {@link Connection} to use for creating the schema.
     *
     * @param recreate <code>true</code> if the schema should be dropped and
     *                 recreated, or <code>false</code> if any existing schema
     *                 should be left in place.
     *
     * @throws SQLException If a JDBC failure occurs.
     */
    @Override
    public void ensureSchema(Connection conn, boolean recreate) throws SQLException {
        List<String> sqlList = new LinkedList<>();

        String createTriggerFunctionSql = "CREATE OR REPLACE FUNCTION sz_maintain_timestamps() "
                + "RETURNS TRIGGER "
                + "LANGUAGE PLPGSQL "
                + "AS $$ "
                + "BEGIN "
                + "  IF (TG_OP = 'UPDATE') THEN "
                + "  BEGIN "
                + "    NEW.created_on := OLD.created_on; "
                + "    NEW.modified_on := CURRENT_TIMESTAMP; "
                + "    return NEW; "
                + "  END; "
                + "ELSIF (TG_OP = 'INSERT') THEN "
                + "  BEGIN "
                + "    NEW.created_on := CURRENT_TIMESTAMP; "
                + "    NEW.modified_on := CURRENT_TIMESTAMP; "
                + "    return NEW; "
                + "  END; "
                + "END IF; "
                + "RETURN NULL; "
                + "END; "
                + "$$;";

        String dropTriggerFunctionSql = "DROP FUNCTION IF EXISTS sz_maintain_timestamps;";

        String createLockTable = "CREATE TABLE IF NOT EXISTS sz_dm_locks ("
                + "  resource_key TEXT NOT NULL PRIMARY KEY, "
                + "  modifier_id TEXT NOT NULL);";

        String dropLockTable = "DROP TABLE IF EXISTS sz_dm_locks;";

        String createEntityTable = "CREATE TABLE IF NOT EXISTS sz_dm_entity ("
                + "  entity_id BIGINT NOT NULL PRIMARY KEY, "
                + "  entity_name TEXT, "
                + "  record_count INTEGER, "
                + "  relation_count INTEGER, "
                + "  entity_hash TEXT, "
                + "  prev_entity_hash TEXT, "
                + "  creator_id TEXT NOT NULL, "
                + "  modifier_id TEXT NOT NULL, "
                + "  created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "  modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP);";

        String dropEntityTable = "DROP TABLE IF EXISTS sz_dm_entity;";

        String createEntityTrigger = formatCreatePostgreSQLTrigger("sz_dm_entity");

        String dropEntityTrigger = formatDropPostgreSQLTrigger("sz_dm_entity");

        String createEntityNewIndex = "CREATE INDEX IF NOT EXISTS sz_dm_entity_new_ix ON sz_dm_entity ("
                + "creator_id);";

        String dropEntityNewIndex = "DROP INDEX IF EXISTS sz_dm_entity_new_ix;";

        String createEntityModIndex = "CREATE INDEX IF NOT EXISTS sz_dm_entity_mod_ix ON sz_dm_entity ("
                + "modifier_id);";

        String dropEntityModIndex = "DROP INDEX IF EXISTS sz_dm_entity_mod_ix;";

        String createRecordTable = "CREATE TABLE IF NOT EXISTS sz_dm_record ("
                + "  data_source TEXT NOT NULL, "
                + "  record_id TEXT NOT NULL, "
                + "  entity_id BIGINT NOT NULL, "
                + "  match_key TEXT, "
                + "  errule_code TEXT, "
                + "  creator_id TEXT NOT NULL, "
                + "  modifier_id TEXT NOT NULL, "
                + "  adopter_id TEXT NULL, "
                + "  created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "  modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "PRIMARY KEY(data_source, record_id));";

        String dropRecordTable = "DROP TABLE IF EXISTS sz_dm_record;";

        String createRecordTrigger = formatCreatePostgreSQLTrigger("sz_dm_record");

        String dropRecordTrigger = formatDropPostgreSQLTrigger("sz_dm_record");

        String createRecordIndex = "CREATE INDEX IF NOT EXISTS sz_dm_record_ix ON sz_dm_record ("
                + "entity_id)";

        String dropRecordIndex = "DROP INDEX IF EXISTS sz_dm_record_ix;";

        String createMatchKeyRecordIndex = "CREATE INDEX IF NOT EXISTS sz_dm_mkey_record_ix ON sz_dm_record ("
                + "match_key, errule_code);";

        String dropMatchKeyRecordIndex = "DROP INDEX IF EXISTS sz_dm_mkey_record_ix;";

        String createPrincipleRecordIndex = "CREATE INDEX IF NOT EXISTS sz_dm_rule_record_ix ON sz_dm_record ("
                + "errule_code, match_key);";

        String dropPrincipleRecordIndex = "DROP INDEX IF EXISTS sz_dm_rule_record_ix;";

        String createRecordNewIndex = "CREATE INDEX IF NOT EXISTS sz_dm_record_new_ix ON sz_dm_record ("
                + "creator_id)";

        String dropRecordNewIndex = "DROP INDEX IF EXISTS sz_dm_record_new_ix;";

        String createRecordModIndex = "CREATE INDEX IF NOT EXISTS sz_dm_record_mod_ix ON sz_dm_record ("
                + "modifier_id)";

        String dropRecordModIndex = "DROP INDEX IF EXISTS sz_dm_record_mod_ix;";

        String createRelationTable = "CREATE TABLE IF NOT EXISTS sz_dm_relation ("
                + "  entity_id BIGINT NOT NULL, "
                + "  related_id BIGINT NOT NULL, "
                + "  match_type TEXT, "
                + "  match_key TEXT, "
                + "  errule_code TEXT, "
                + "  relation_hash TEXT, "
                + "  prev_relation_hash TEXT, "
                + "  creator_id TEXT NOT NULL, "
                + "  modifier_id TEXT NOT NULL, "
                + "  created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "  modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "PRIMARY KEY(entity_id, related_id));";

        String dropRelationTable = "DROP TABLE IF EXISTS sz_dm_relation;";

        String createRelationIndex = "CREATE INDEX IF NOT EXISTS sz_dm_relation_ix ON sz_dm_relation ("
                + "related_id, entity_id);";

        String dropRelationIndex = "DROP INDEX IF EXISTS sz_dm_relation_ix;";

        String createMatchKeyRelationIndex = "CREATE INDEX IF NOT EXISTS sz_dm_mkey_relation_ix ON sz_dm_relation ("
                + "match_key, errule_code);";

        String dropMatchKeyRelationIndex = "DROP INDEX IF EXISTS sz_dm_mkey_relation_ix;";

        String createPrincipleRelationIndex = "CREATE INDEX IF NOT EXISTS sz_dm_rule_relation_ix ON sz_dm_relation ("
                + "errule_code, match_key);";

        String dropPrincipleRelationIndex = "DROP INDEX IF EXISTS sz_dm_rule_relation_ix;";

        String createRelationNewIndex = "CREATE INDEX IF NOT EXISTS sz_dm_relation_new_ix ON sz_dm_relation ("
                + "creator_id);";

        String dropRelationNewIndex = "DROP INDEX IF EXISTS sz_dm_relation_new_ix;";

        String createRelationModIndex = "CREATE INDEX IF NOT EXISTS sz_dm_relation_mod_ix ON sz_dm_relation ("
                + "modifier_id);";

        String dropRelationModIndex = "DROP INDEX IF EXISTS sz_dm_relation_mod_ix;";

        String createRelationTrigger = formatCreatePostgreSQLTrigger("sz_dm_relation");

        String dropRelationTrigger = formatDropPostgreSQLTrigger("sz_dm_relation");

        String createReportTable = "CREATE TABLE IF NOT EXISTS sz_dm_report ("
                + "  report_key TEXT NOT NULL PRIMARY KEY, "
                + "  report TEXT NOT NULL, "
                + "  statistic TEXT NOT NULL, "
                + "  data_source1 TEXT, "
                + "  data_source2 TEXT, "
                + "  entity_count BIGINT, "
                + "  record_count BIGINT, "
                + "  relation_count BIGINT, "
                + "  report_notes TEXT, "
                + "  created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "  modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP);";

        String dropReportTable = "DROP TABLE IF EXISTS sz_dm_report;";

        String createReportTrigger = formatCreatePostgreSQLTrigger("sz_dm_report");

        String dropReportTrigger = formatDropPostgreSQLTrigger("sz_dm_report");

        String createReportDetailTable = "CREATE TABLE IF NOT EXISTS sz_dm_report_detail ("
                + "  report_key TEXT NOT NULL, "
                + "  entity_id BIGINT NOT NULL, "
                + "  related_id BIGINT NOT NULL DEFAULT (0), "
                + "  stat_count INTEGER DEFAULT (0), "
                + "  report_notes TEXT, "
                + "  creator_id TEXT NOT NULL, "
                + "  modifier_id TEXT NOT NULL, "
                + "  created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "  modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "PRIMARY KEY(report_key, entity_id, related_id));";

        String dropReportDetailTable = "DROP TABLE IF EXISTS sz_dm_report_detail;";

        String createReportDetailIndex1 = "CREATE UNIQUE INDEX IF NOT EXISTS sz_dm_rpt_detail_uix1 "
                + "ON sz_dm_report_detail (entity_id, related_id, report_key);";

        String dropReportDetailIndex1 = "DROP INDEX IF EXISTS sz_dm_rpt_detail_uix1;";

        String createReportDetailIndex2 = "CREATE UNIQUE INDEX IF NOT EXISTS sz_dm_rpt_detail_uix2 "
                + "ON sz_dm_report_detail (related_id, entity_id, report_key);";

        String dropReportDetailIndex2 = "DROP INDEX IF EXISTS sz_dm_rpt_detail_uix2;";

        String createReportDetailNewIndex = "CREATE INDEX IF NOT EXISTS sz_dm_rpt_det_new_ix "
                + "ON sz_dm_report_detail (creator_id);";

        String dropReportDetailNewIndex = "DROP INDEX IF EXISTS sz_dm_rpt_det_new_ix;";

        String createReportDetailModIndex = "CREATE INDEX IF NOT EXISTS sz_dm_rpt_det_mod_ix "
                + "ON sz_dm_report_detail (modifier_id);";

        String dropReportDetailModIndex = "DROP INDEX IF EXISTS sz_dm_rpt_det_mod_ix;";

        String createReportDetailTrigger = formatCreatePostgreSQLTrigger("sz_dm_report_detail");

        String dropReportDetailTrigger = formatDropPostgreSQLTrigger("sz_dm_report_detail");

        String createPendingReportTable = "CREATE TABLE IF NOT EXISTS sz_dm_pending_report ("
                + "  report_key TEXT NOT NULL, "
                + "  lease_id TEXT, "
                + "  expire_lease_at TIMESTAMP, "
                + "  entity_delta INTEGER, "
                + "  record_delta INTEGER, "
                + "  relation_delta INTEGER, "
                + "  entity_id BIGINT, "
                + "  related_id BIGINT, "
                + "  created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "  modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP);";

        String dropPendingReportTable = "DROP TABLE IF EXISTS sz_dm_pending_report;";

        String createPendingReportIndex1 = "CREATE INDEX IF NOT EXISTS sz_dm_pend_rpt_ix1 "
                + "ON sz_dm_pending_report (report_key, lease_id, expire_lease_at);";

        String dropPendingReportIndex1 = "DROP INDEX IF EXISTS sz_dm_pend_rpt_ix1;";

        String createPendingReportIndex2 = "CREATE INDEX IF NOT EXISTS sz_dm_pend_rpt_ix2 "
                + "ON sz_dm_pending_report (expire_lease_at, lease_id);";

        String dropPendingReportIndex2 = "DROP INDEX IF EXISTS sz_dm_pend_rpt_ix2;";

        String createPendingReportIndex3 = "CREATE INDEX IF NOT EXISTS sz_dm_pend_rpt_ix3 "
                + "ON sz_dm_pending_report (lease_id);";

        String dropPendingReportIndex3 = "DROP INDEX IF EXISTS sz_dm_pend_rpt_ix3;";

        String createPendingReportTrigger = formatCreatePostgreSQLTrigger("sz_dm_pending_report");

        String dropPendingReportTrigger = formatDropPostgreSQLTrigger("sz_dm_pending_report");

        if (recreate) {
            sqlList.add(dropLockTable);

            sqlList.add(dropEntityModIndex);
            sqlList.add(dropEntityNewIndex);
            sqlList.add(dropEntityTrigger);
            sqlList.add(dropEntityTable);

            sqlList.add(dropRecordNewIndex);
            sqlList.add(dropRecordModIndex);
            sqlList.add(dropPrincipleRecordIndex);
            sqlList.add(dropMatchKeyRecordIndex);
            sqlList.add(dropRecordIndex);
            sqlList.add(dropRecordTrigger);
            sqlList.add(dropRecordTable);

            sqlList.add(dropRelationModIndex);
            sqlList.add(dropRelationNewIndex);
            sqlList.add(dropPrincipleRelationIndex);
            sqlList.add(dropMatchKeyRelationIndex);
            sqlList.add(dropRelationIndex);
            sqlList.add(dropRelationTrigger);
            sqlList.add(dropRelationTable);

            sqlList.add(dropReportTrigger);
            sqlList.add(dropReportTable);

            sqlList.add(dropReportDetailNewIndex);
            sqlList.add(dropReportDetailModIndex);
            sqlList.add(dropReportDetailIndex2);
            sqlList.add(dropReportDetailIndex1);
            sqlList.add(dropReportDetailTrigger);
            sqlList.add(dropReportDetailTable);

            sqlList.add(dropPendingReportIndex3);
            sqlList.add(dropPendingReportIndex2);
            sqlList.add(dropPendingReportIndex1);
            sqlList.add(dropPendingReportTrigger);
            sqlList.add(dropPendingReportTable);

            sqlList.add(dropTriggerFunctionSql);
        }
        sqlList.add(createTriggerFunctionSql);
        sqlList.add(createLockTable);
        sqlList.add(createEntityTable);
        sqlList.add(createEntityNewIndex);
        sqlList.add(createEntityModIndex);
        sqlList.add(dropEntityTrigger);
        sqlList.add(createEntityTrigger);

        sqlList.add(createRecordTable);
        sqlList.add(dropRecordTrigger);
        sqlList.add(createRecordTrigger);
        sqlList.add(createRecordIndex);
        sqlList.add(createMatchKeyRecordIndex);
        sqlList.add(createPrincipleRecordIndex);
        sqlList.add(createRecordNewIndex);
        sqlList.add(createRecordModIndex);

        sqlList.add(createRelationTable);
        sqlList.add(dropRelationTrigger);
        sqlList.add(createRelationTrigger);
        sqlList.add(createRelationIndex);
        sqlList.add(createMatchKeyRelationIndex);
        sqlList.add(createPrincipleRelationIndex);
        sqlList.add(createRelationNewIndex);
        sqlList.add(createRelationModIndex);

        sqlList.add(createReportTable);
        sqlList.add(dropReportTrigger);
        sqlList.add(createReportTrigger);

        sqlList.add(createReportDetailTable);
        sqlList.add(dropReportDetailTrigger);
        sqlList.add(createReportDetailTrigger);
        sqlList.add(createReportDetailIndex1);
        sqlList.add(createReportDetailIndex2);
        sqlList.add(createReportDetailNewIndex);
        sqlList.add(createReportDetailModIndex);

        sqlList.add(createPendingReportTable);
        sqlList.add(dropPendingReportTrigger);
        sqlList.add(createPendingReportTrigger);
        sqlList.add(createPendingReportIndex1);
        sqlList.add(createPendingReportIndex2);
        sqlList.add(createPendingReportIndex3);

        if (!conn.getAutoCommit()) {
            sqlList.add("COMMIT;");
        }

        this.executeStatements(conn, sqlList);
    }

    /**
     * Formats a PostgreSQL create trigger statement for the timestamp maintenance
     * trigger for the specified table name.
     *
     * @param tableName The table name for the create trigger statement.
     * @return The create trigger statement.
     */
    protected String formatCreatePostgreSQLTrigger(String tableName) {
        return "CREATE TRIGGER " + tableName + "_trig "
                + "BEFORE INSERT OR UPDATE "
                + "ON " + tableName + " "
                + "FOR EACH ROW "
                + "WHEN (pg_trigger_depth() = 0) "
                + "EXECUTE PROCEDURE sz_maintain_timestamps();";
    }

    /**
     * Formats a PostgreSQL drop trigger statement for the timestamp maintenance
     * trigger for the specified table name.
     *
     * @param tableName The table name for the drop trigger statement.
     * @return The drop trigger statement.
     */
    protected String formatDropPostgreSQLTrigger(String tableName) {
        return "DROP TRIGGER IF EXISTS " + tableName + "_trig ON "
                + tableName + ";";
    }
}
