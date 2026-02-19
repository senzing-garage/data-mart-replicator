package com.senzing.datamart.schema;

import org.junit.jupiter.api.*;

import java.io.File;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link SQLiteSchemaBuilder} using a temporary SQLite database file.
 */
class SQLiteSchemaBuilderTest extends AbstractSchemaBuilderTest<SQLiteSchemaBuilder> {

    private File tempDbFile;

    /**
     * Expected index names created by the schema builder.
     */
    private static final Set<String> EXPECTED_INDEXES = new HashSet<>(Arrays.asList(
            "sz_dm_entity_new_ix",
            "sz_dm_entity_mod_ix",
            "sz_dm_record_ix",
            "sz_dm_mkey_record_ix",
            "sz_dm_rule_record_ix",
            "sz_dm_record_new_ix",
            "sz_dm_record_mod_ix",
            "sz_dm_relation_ix",
            "sz_dm_mkey_relation_ix",
            "sz_dm_rev_mkey_rel_ix",
            "sz_dm_rule_relation_ix",
            "sz_dm_rev_rule_rel_ix",
            "sz_dm_relation_new_ix",
            "sz_dm_relation_mod_ix",
            "sz_dm_rpt_detail_ix",
            "sz_dm_rpt_detail_uix",
            "sz_dm_rpt_det_new_ix",
            "sz_dm_rpt_det_mod_ix",
            "sz_dm_pend_rpt_ix1",
            "sz_dm_pend_rpt_ix2"
    ));

    /**
     * Expected trigger names created by the schema builder (SQLite has insert and update triggers).
     */
    private static final Set<String> EXPECTED_TRIGGERS = new HashSet<>(Arrays.asList(
            "sz_dm_entity_new",
            "sz_dm_entity_mod",
            "sz_dm_record_new",
            "sz_dm_record_mod",
            "sz_dm_relation_new",
            "sz_dm_relation_mod",
            "sz_dm_report_new",
            "sz_dm_report_mod",
            "sz_dm_report_detail_new",
            "sz_dm_report_detail_mod",
            "sz_dm_pending_report_new",
            "sz_dm_pending_report_mod"
    ));

    @BeforeAll
    void setUp() throws Exception {
        // Create a temporary database file
        tempDbFile = File.createTempFile("sqlite_test_", ".db");
        tempDbFile.deleteOnExit();

        // Connect to the SQLite database
        String jdbcUrl = "jdbc:sqlite:" + tempDbFile.getAbsolutePath();
        connection = DriverManager.getConnection(jdbcUrl);
        connection.setAutoCommit(false);

        schemaBuilder = new SQLiteSchemaBuilder();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    @Override
    protected SQLiteSchemaBuilder getSchemaBuilder() {
        return schemaBuilder;
    }

    @Override
    protected Set<String> getExpectedIndexes() {
        return EXPECTED_INDEXES;
    }

    @Override
    protected Set<String> getExpectedTriggers() {
        return EXPECTED_TRIGGERS;
    }

    @Override
    protected Set<String> getExistingTables() throws SQLException {
        Set<String> tables = new HashSet<>();
        String sql = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getString("name"));
            }
        }
        return tables;
    }

    @Override
    protected Set<String> getExistingIndexes() throws SQLException {
        Set<String> indexes = new HashSet<>();
        String sql = "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String name = rs.getString("name");
                if (name != null) {
                    indexes.add(name);
                }
            }
        }
        return indexes;
    }

    @Override
    protected Set<String> getExistingTriggers() throws SQLException {
        Set<String> triggers = new HashSet<>();
        String sql = "SELECT name FROM sqlite_master WHERE type = 'trigger'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                triggers.add(rs.getString("name"));
            }
        }
        return triggers;
    }
}
