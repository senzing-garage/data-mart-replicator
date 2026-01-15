package com.senzing.datamart.schema;

import org.junit.jupiter.api.*;

import java.io.File;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SQLiteSchemaBuilder} using a temporary SQLite database file.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SQLiteSchemaBuilderTest {

    private Connection connection;
    private SQLiteSchemaBuilder schemaBuilder;
    private File tempDbFile;

    /**
     * Expected table names created by the schema builder.
     */
    private static final Set<String> EXPECTED_TABLES = new HashSet<>(Arrays.asList(
            "sz_dm_locks",
            "sz_dm_entity",
            "sz_dm_record",
            "sz_dm_relation",
            "sz_dm_report",
            "sz_dm_report_detail",
            "sz_dm_pending_report"
    ));

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
            "sz_dm_rule_relation_ix",
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

    /**
     * Test 1: Create schema on empty database with recreate=false.
     * Verifies all expected database objects are created.
     */
    @Test
    @Order(1)
    void testEnsureSchemaOnEmptyDatabase() throws SQLException {
        // Call ensureSchema with recreate=false on empty database
        assertDoesNotThrow(() -> schemaBuilder.ensureSchema(connection, false));

        // Verify all database objects exist
        verifyAllDatabaseObjectsExist();
    }

    /**
     * Test 2: Insert rows, then call ensureSchema with recreate=true.
     * Verifies tables are recreated (empty) and all objects still exist.
     */
    @Test
    @Order(2)
    void testEnsureSchemaWithRecreateTrue() throws SQLException {
        // Insert test rows into tables
        insertTestData();

        // Verify rows were inserted
        assertTrue(getRowCount("sz_dm_entity") > 0, "Entity table should have rows before recreate");
        assertTrue(getRowCount("sz_dm_record") > 0, "Record table should have rows before recreate");

        // Call ensureSchema with recreate=true
        assertDoesNotThrow(() -> schemaBuilder.ensureSchema(connection, true));

        // Verify all database objects still exist
        verifyAllDatabaseObjectsExist();

        // Verify all tables are now empty (data was cleared)
        assertEquals(0, getRowCount("sz_dm_locks"), "Locks table should be empty after recreate");
        assertEquals(0, getRowCount("sz_dm_entity"), "Entity table should be empty after recreate");
        assertEquals(0, getRowCount("sz_dm_record"), "Record table should be empty after recreate");
        assertEquals(0, getRowCount("sz_dm_relation"), "Relation table should be empty after recreate");
        assertEquals(0, getRowCount("sz_dm_report"), "Report table should be empty after recreate");
        assertEquals(0, getRowCount("sz_dm_report_detail"), "Report detail table should be empty after recreate");
        assertEquals(0, getRowCount("sz_dm_pending_report"), "Pending report table should be empty after recreate");
    }

    /**
     * Test 3: Call ensureSchema with recreate=false on existing schema.
     * Verifies no exception is thrown and all objects remain.
     */
    @Test
    @Order(3)
    void testEnsureSchemaOnExistingSchemaNoRecreate() throws SQLException {
        // Insert some test data first
        insertTestData();
        long entityCountBefore = getRowCount("sz_dm_entity");
        assertTrue(entityCountBefore > 0, "Should have test data before calling ensureSchema");

        // Call ensureSchema with recreate=false - should not throw
        assertDoesNotThrow(() -> schemaBuilder.ensureSchema(connection, false));

        // Verify all database objects still exist
        verifyAllDatabaseObjectsExist();

        // Verify data was NOT cleared (since recreate=false)
        long entityCountAfter = getRowCount("sz_dm_entity");
        assertEquals(entityCountBefore, entityCountAfter,
                "Data should be preserved when recreate=false");
    }

    /**
     * Verifies that all expected database objects (tables, indexes, triggers) exist.
     */
    private void verifyAllDatabaseObjectsExist() throws SQLException {
        // Verify tables
        Set<String> existingTables = getExistingTables();
        for (String expectedTable : EXPECTED_TABLES) {
            assertTrue(existingTables.contains(expectedTable),
                    "Expected table not found: " + expectedTable);
        }

        // Verify indexes
        Set<String> existingIndexes = getExistingIndexes();
        for (String expectedIndex : EXPECTED_INDEXES) {
            assertTrue(existingIndexes.contains(expectedIndex),
                    "Expected index not found: " + expectedIndex);
        }

        // Verify triggers
        Set<String> existingTriggers = getExistingTriggers();
        for (String expectedTrigger : EXPECTED_TRIGGERS) {
            assertTrue(existingTriggers.contains(expectedTrigger),
                    "Expected trigger not found: " + expectedTrigger);
        }
    }

    /**
     * Gets the set of existing table names in the database.
     */
    private Set<String> getExistingTables() throws SQLException {
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

    /**
     * Gets the set of existing index names in the database.
     */
    private Set<String> getExistingIndexes() throws SQLException {
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

    /**
     * Gets the set of existing trigger names in the database.
     */
    private Set<String> getExistingTriggers() throws SQLException {
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

    /**
     * Gets the row count for a table.
     */
    private long getRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * Inserts test data into the schema tables.
     */
    private void insertTestData() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Insert into sz_dm_entity
            stmt.execute("INSERT INTO sz_dm_entity (entity_id, entity_name, record_count, "
                    + "relation_count, creator_id, modifier_id) "
                    + "VALUES (1, 'Test Entity', 1, 0, 'test', 'test')");

            // Insert into sz_dm_record
            stmt.execute("INSERT INTO sz_dm_record (data_source, record_id, entity_id, "
                    + "creator_id, modifier_id) "
                    + "VALUES ('TEST_DS', 'REC001', 1, 'test', 'test')");

            // Insert into sz_dm_locks
            stmt.execute("INSERT INTO sz_dm_locks (resource_key, modifier_id) "
                    + "VALUES ('lock1', 'test')");

            connection.commit();
        }
    }
}
