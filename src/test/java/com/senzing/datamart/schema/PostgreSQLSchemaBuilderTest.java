package com.senzing.datamart.schema;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PostgreSQLSchemaBuilder} using Zonky embedded PostgreSQL.
 */
class PostgreSQLSchemaBuilderTest extends AbstractSchemaBuilderTest<PostgreSQLSchemaBuilder> {

    private EmbeddedPostgres embeddedPostgres;

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
            "sz_dm_rpt_detail_uix1",
            "sz_dm_rpt_detail_uix2",
            "sz_dm_rpt_det_new_ix",
            "sz_dm_rpt_det_mod_ix",
            "sz_dm_pend_rpt_ix1",
            "sz_dm_pend_rpt_ix2",
            "sz_dm_pend_rpt_ix3"
    ));

    /**
     * Expected trigger names created by the schema builder.
     */
    private static final Set<String> EXPECTED_TRIGGERS = new HashSet<>(Arrays.asList(
            "sz_dm_entity_trig",
            "sz_dm_record_trig",
            "sz_dm_relation_trig",
            "sz_dm_report_trig",
            "sz_dm_report_detail_trig",
            "sz_dm_pending_report_trig"
    ));

    /**
     * Expected function names created by the schema builder.
     */
    private static final Set<String> EXPECTED_FUNCTIONS = new HashSet<>(Arrays.asList(
            "sz_maintain_timestamps"
    ));

    @BeforeAll
    void setUp() throws Exception {
        embeddedPostgres = EmbeddedPostgres.builder().start();
        connection = embeddedPostgres.getPostgresDatabase().getConnection();
        connection.setAutoCommit(false);
        schemaBuilder = new PostgreSQLSchemaBuilder();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (embeddedPostgres != null) {
            embeddedPostgres.close();
        }
    }

    @Override
    protected PostgreSQLSchemaBuilder getSchemaBuilder() {
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
    protected void verifyAllDatabaseObjectsExist() throws SQLException {
        // Call base verification
        super.verifyAllDatabaseObjectsExist();

        // Additionally verify functions (PostgreSQL-specific)
        Set<String> existingFunctions = getExistingFunctions();
        for (String expectedFunction : EXPECTED_FUNCTIONS) {
            assertTrue(existingFunctions.contains(expectedFunction),
                    "Expected function not found: " + expectedFunction);
        }
    }

    @Override
    protected Set<String> getExistingTables() throws SQLException {
        Set<String> tables = new HashSet<>();
        String sql = "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        }
        return tables;
    }

    @Override
    protected Set<String> getExistingIndexes() throws SQLException {
        Set<String> indexes = new HashSet<>();
        String sql = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                indexes.add(rs.getString("indexname"));
            }
        }
        return indexes;
    }

    @Override
    protected Set<String> getExistingTriggers() throws SQLException {
        Set<String> triggers = new HashSet<>();
        String sql = "SELECT trigger_name FROM information_schema.triggers "
                + "WHERE trigger_schema = 'public'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                triggers.add(rs.getString("trigger_name"));
            }
        }
        return triggers;
    }

    /**
     * Gets the set of existing function names.
     */
    private Set<String> getExistingFunctions() throws SQLException {
        Set<String> functions = new HashSet<>();
        String sql = "SELECT routine_name FROM information_schema.routines "
                + "WHERE routine_schema = 'public' AND routine_type = 'FUNCTION'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                functions.add(rs.getString("routine_name"));
            }
        }
        return functions;
    }
}
