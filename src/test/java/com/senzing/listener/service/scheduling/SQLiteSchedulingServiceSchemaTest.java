package com.senzing.listener.service.scheduling;

import org.junit.jupiter.api.*;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SQLiteSchedulingService} ensureSchema functionality.
 * These tests MUST run in order as they build on each other.
 * Uses a temporary file-based SQLite database.
 */
class SQLiteSchedulingServiceSchemaTest
    extends AbstractSchedulingServiceSchemaTest<SQLiteSchedulingService> {

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

    @Override
    protected SQLiteSchedulingService getService() {
        return service;
    }

    @Override
    protected void ensureConnectionOpen() throws SQLException {
        if (testConnection == null || testConnection.isClosed()) {
            testConnection = DriverManager.getConnection(jdbcUrl);
            testConnection.setAutoCommit(false);
        }
    }

    @Override
    protected String getSchemaName() {
        return null;
    }

    @Override
    protected void verifyAllDatabaseObjectsExist() throws SQLException {
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

    @Override
    protected boolean tableExists(String tableName) throws SQLException {
        DatabaseMetaData meta = testConnection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    @Override
    protected boolean indexExists(String indexName) throws SQLException {
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
}
