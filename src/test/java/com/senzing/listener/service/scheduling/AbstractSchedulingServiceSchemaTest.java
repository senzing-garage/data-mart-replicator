package com.senzing.listener.service.scheduling;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract base test class for testing SchedulingService schema functionality.
 * Provides common test methods that work across database implementations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractSchedulingServiceSchemaTest<S extends AbstractSQLSchedulingService> {

    protected Connection testConnection;
    protected S service;

    /**
     * Returns the scheduling service under test.
     */
    protected abstract S getService();

    /**
     * Reopens the test connection if it was closed.
     */
    protected abstract void ensureConnectionOpen() throws SQLException;

    /**
     * Verifies that all expected database objects exist.
     */
    protected abstract void verifyAllDatabaseObjectsExist() throws SQLException;

    /**
     * Checks if a table exists using database-specific approach.
     */
    protected abstract boolean tableExists(String tableName) throws SQLException;

    /**
     * Checks if an index exists using database-specific approach.
     */
    protected abstract boolean indexExists(String indexName) throws SQLException;

    /**
     * Returns the schema name for metadata queries (null for SQLite, "public" for PostgreSQL).
     */
    protected abstract String getSchemaName();

    /**
     * Verifies the expected columns exist in the table.
     */
    protected void verifyTableColumns() throws SQLException {
        DatabaseMetaData meta = testConnection.getMetaData();
        Set<String> expectedColumns = new HashSet<>(Arrays.asList(
            "task_id", "signature", "allow_collapse_flag", "lease_id",
            "expire_lease_at", "multiplicity", "json_text", "created_on", "modified_on"
        ));

        Set<String> actualColumns = new HashSet<>();
        try (ResultSet rs = meta.getColumns(null, getSchemaName(), "sz_follow_up_tasks", null)) {
            while (rs.next()) {
                actualColumns.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
        }

        for (String expectedCol : expectedColumns) {
            assertTrue(actualColumns.contains(expectedCol),
                "Column " + expectedCol + " should exist in sz_follow_up_tasks");
        }
    }

    protected int getRowCount(String tableName) throws SQLException {
        ensureConnectionOpen();
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Statement stmt = testConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    protected void insertTestRow() throws SQLException {
        ensureConnectionOpen();
        String sql = "INSERT INTO sz_follow_up_tasks (signature, json_text) VALUES (?, ?)";
        try (PreparedStatement pstmt = testConnection.prepareStatement(sql)) {
            pstmt.setString(1, "test_signature_" + System.nanoTime());
            pstmt.setString(2, "{\"action\":\"TEST\"}");
            pstmt.executeUpdate();
        }
        testConnection.commit();
    }

    @Test
    @Order(100)
    void testEnsureSchemaOnEmptyDatabase() throws Exception {
        // Call ensureSchema - this will close the connection
        getService().ensureSchema(false);

        // Reopen connection to verify
        ensureConnectionOpen();

        // Verify all database objects exist
        verifyAllDatabaseObjectsExist();
    }

    @Test
    @Order(200)
    void testEnsureSchemaWithRecreateTrue() throws Exception {
        ensureConnectionOpen();

        // Insert test data
        insertTestRow();

        // Verify data was inserted
        assertTrue(getRowCount("sz_follow_up_tasks") > 0, "Table should have rows before recreate");

        // Call ensureSchema with recreate=true
        getService().ensureSchema(true);
        ensureConnectionOpen();

        // Verify all database objects still exist
        verifyAllDatabaseObjectsExist();

        // Verify table is now empty (data was cleared)
        assertEquals(0, getRowCount("sz_follow_up_tasks"), "Table should be empty after recreate");
    }

    @Test
    @Order(300)
    void testEnsureSchemaOnExistingSchema() throws Exception {
        ensureConnectionOpen();

        // Insert test data
        insertTestRow();

        // Verify data was inserted
        int beforeCount = getRowCount("sz_follow_up_tasks");
        assertTrue(beforeCount > 0, "Table should have rows before ensureSchema");

        // Call ensureSchema with recreate=false on existing schema
        getService().ensureSchema(false);
        ensureConnectionOpen();

        // Verify all database objects still exist
        verifyAllDatabaseObjectsExist();

        // Verify data was NOT cleared (recreate=false)
        assertEquals(beforeCount, getRowCount("sz_follow_up_tasks"),
            "Table should still have same rows after ensureSchema(false)");
    }
}
