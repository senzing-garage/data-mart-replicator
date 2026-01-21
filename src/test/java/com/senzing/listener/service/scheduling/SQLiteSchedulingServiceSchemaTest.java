package com.senzing.listener.service.scheduling;

import org.junit.jupiter.api.*;

import java.io.File;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SQLiteSchedulingService} ensureSchema functionality.
 * These tests MUST run in order as they build on each other.
 * Uses a temporary file-based SQLite database.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SQLiteSchedulingServiceSchemaTest {

    private Connection testConnection;
    private SQLiteSchedulingService service;
    private File tempDbFile;
    private String jdbcUrl;

    @BeforeAll
    void setUp() throws Exception {
        // Create a temporary database file
        tempDbFile = File.createTempFile("sqlite_scheduling_schema_", ".db");
        tempDbFile.deleteOnExit();
        jdbcUrl = "jdbc:sqlite:" + tempDbFile.getAbsolutePath();

        // Create persistent test connection
        testConnection = DriverManager.getConnection(jdbcUrl);
        testConnection.setAutoCommit(false);

        // Create service that uses the same connection
        service = new SQLiteSchedulingService() {
            @Override
            protected Connection getConnection() throws SQLException {
                // Return the test connection - ensureSchema will close it,
                // but we'll reopen it in each test
                return testConnection;
            }
        };
    }

    @AfterAll
    void tearDown() throws Exception {
        if (testConnection != null && !testConnection.isClosed()) {
            testConnection.close();
        }
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    /**
     * Reopens the test connection if it was closed.
     */
    private void ensureConnectionOpen() throws SQLException {
        if (testConnection == null || testConnection.isClosed()) {
            testConnection = DriverManager.getConnection(jdbcUrl);
            testConnection.setAutoCommit(false);
        }
    }

    /**
     * Verifies that all expected database objects (table, indexes, triggers) exist.
     */
    private void verifyAllDatabaseObjectsExist() throws SQLException {
        ensureConnectionOpen();

        // Verify table exists
        assertTrue(tableExists("sz_follow_up_tasks"), "Table sz_follow_up_tasks should exist");

        // Verify indexes exist
        assertTrue(indexExists("sz_task_dup"), "Index sz_task_dup should exist");
        assertTrue(indexExists("sz_task_lease"), "Index sz_task_lease should exist");

        // Verify triggers exist
        assertTrue(triggerExists("sz_follow_up_tasks_mod"), "Trigger sz_follow_up_tasks_mod should exist");
        assertTrue(triggerExists("sz_follow_up_tasks_create"), "Trigger sz_follow_up_tasks_create should exist");

        // Verify table structure
        verifyTableColumns();
    }

    private boolean tableExists(String tableName) throws SQLException {
        DatabaseMetaData meta = testConnection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    private boolean indexExists(String indexName) throws SQLException {
        String sql = "SELECT name FROM sqlite_master WHERE type='index' AND name=?";
        try (PreparedStatement pstmt = testConnection.prepareStatement(sql)) {
            pstmt.setString(1, indexName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean triggerExists(String triggerName) throws SQLException {
        String sql = "SELECT name FROM sqlite_master WHERE type='trigger' AND name=?";
        try (PreparedStatement pstmt = testConnection.prepareStatement(sql)) {
            pstmt.setString(1, triggerName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void verifyTableColumns() throws SQLException {
        DatabaseMetaData meta = testConnection.getMetaData();
        Set<String> expectedColumns = new HashSet<>(Arrays.asList(
            "task_id", "signature", "allow_collapse_flag", "lease_id",
            "expire_lease_at", "multiplicity", "json_text", "created_on", "modified_on"
        ));

        Set<String> actualColumns = new HashSet<>();
        try (ResultSet rs = meta.getColumns(null, null, "sz_follow_up_tasks", null)) {
            while (rs.next()) {
                actualColumns.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
        }

        for (String expectedCol : expectedColumns) {
            assertTrue(actualColumns.contains(expectedCol),
                "Column " + expectedCol + " should exist in sz_follow_up_tasks");
        }
    }

    private int getRowCount(String tableName) throws SQLException {
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

    private void insertTestRow() throws SQLException {
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
        service.ensureSchema(false);

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
        service.ensureSchema(true);
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
        service.ensureSchema(false);
        ensureConnectionOpen();

        // Verify all database objects still exist
        verifyAllDatabaseObjectsExist();

        // Verify data was NOT cleared (recreate=false)
        assertEquals(beforeCount, getRowCount("sz_follow_up_tasks"),
            "Table should still have same rows after ensureSchema(false)");
    }
}
