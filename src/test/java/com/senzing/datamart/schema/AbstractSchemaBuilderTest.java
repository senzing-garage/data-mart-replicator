package com.senzing.datamart.schema;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract base test class for testing SchemaBuilder implementations.
 * Provides common test methods that work across database implementations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractSchemaBuilderTest<S extends SchemaBuilder> {

    protected Connection connection;
    protected S schemaBuilder;

    /**
     * Expected table names created by the schema builder.
     */
    protected static final Set<String> EXPECTED_TABLES = new HashSet<>(Arrays.asList(
            "sz_dm_locks",
            "sz_dm_entity",
            "sz_dm_record",
            "sz_dm_relation",
            "sz_dm_report",
            "sz_dm_report_detail",
            "sz_dm_pending_report"
    ));

    /**
     * Returns the schema builder under test.
     */
    protected abstract S getSchemaBuilder();

    /**
     * Returns the expected index names for this database type.
     */
    protected abstract Set<String> getExpectedIndexes();

    /**
     * Returns the expected trigger names for this database type.
     */
    protected abstract Set<String> getExpectedTriggers();

    /**
     * Gets the set of existing table names in the database.
     */
    protected abstract Set<String> getExistingTables() throws SQLException;

    /**
     * Gets the set of existing index names in the database.
     */
    protected abstract Set<String> getExistingIndexes() throws SQLException;

    /**
     * Gets the set of existing trigger names in the database.
     */
    protected abstract Set<String> getExistingTriggers() throws SQLException;

    /**
     * Verifies that all expected database objects exist.
     * Subclasses may override to add additional verification (e.g., functions).
     */
    protected void verifyAllDatabaseObjectsExist() throws SQLException {
        // Verify tables
        Set<String> existingTables = getExistingTables();
        for (String expectedTable : EXPECTED_TABLES) {
            assertTrue(existingTables.contains(expectedTable),
                    "Expected table not found: " + expectedTable);
        }

        // Verify indexes
        Set<String> existingIndexes = getExistingIndexes();
        for (String expectedIndex : getExpectedIndexes()) {
            assertTrue(existingIndexes.contains(expectedIndex),
                    "Expected index not found: " + expectedIndex);
        }

        // Verify triggers
        Set<String> existingTriggers = getExistingTriggers();
        for (String expectedTrigger : getExpectedTriggers()) {
            assertTrue(existingTriggers.contains(expectedTrigger),
                    "Expected trigger not found: " + expectedTrigger);
        }
    }

    /**
     * Gets the row count for a table.
     */
    protected long getRowCount(String tableName) throws SQLException {
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
    protected void insertTestData() throws SQLException {
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

    /**
     * Test 1: Create schema on empty database with recreate=false.
     * Verifies all expected database objects are created.
     */
    @Test
    @Order(100)
    void testEnsureSchemaOnEmptyDatabase() throws SQLException {
        // Call ensureSchema with recreate=false on empty database
        assertDoesNotThrow(() -> getSchemaBuilder().ensureSchema(connection, false));

        // Verify all database objects exist
        verifyAllDatabaseObjectsExist();
    }

    /**
     * Test 2: Insert rows, then call ensureSchema with recreate=true.
     * Verifies tables are recreated (empty) and all objects still exist.
     */
    @Test
    @Order(200)
    void testEnsureSchemaWithRecreateTrue() throws SQLException {
        // Insert test rows into tables
        insertTestData();

        // Verify rows were inserted
        assertTrue(getRowCount("sz_dm_entity") > 0, "Entity table should have rows before recreate");
        assertTrue(getRowCount("sz_dm_record") > 0, "Record table should have rows before recreate");

        // Call ensureSchema with recreate=true
        assertDoesNotThrow(() -> getSchemaBuilder().ensureSchema(connection, true));

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
    @Order(300)
    void testEnsureSchemaOnExistingSchemaNoRecreate() throws SQLException {
        // Insert some test data first
        insertTestData();
        long entityCountBefore = getRowCount("sz_dm_entity");
        assertTrue(entityCountBefore > 0, "Should have test data before calling ensureSchema");

        // Call ensureSchema with recreate=false - should not throw
        assertDoesNotThrow(() -> getSchemaBuilder().ensureSchema(connection, false));

        // Verify all database objects still exist
        verifyAllDatabaseObjectsExist();

        // Verify data was NOT cleared (since recreate=false)
        long entityCountAfter = getRowCount("sz_dm_entity");
        assertEquals(entityCountBefore, entityCountAfter,
                "Data should be preserved when recreate=false");
    }
}
