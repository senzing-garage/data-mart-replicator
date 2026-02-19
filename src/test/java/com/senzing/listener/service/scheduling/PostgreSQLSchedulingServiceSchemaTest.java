package com.senzing.listener.service.scheduling;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PostgreSQLSchedulingService} ensureSchema functionality.
 * These tests MUST run in order as they build on each other.
 * Uses Zonky Embedded PostgreSQL.
 */
class PostgreSQLSchedulingServiceSchemaTest
    extends AbstractSchedulingServiceSchemaTest<PostgreSQLSchedulingService> {

    private EmbeddedPostgres embeddedPostgres;

    @BeforeAll
    void setUp() throws Exception {
        // Start embedded PostgreSQL
        embeddedPostgres = EmbeddedPostgres.builder().start();

        // Get test connection
        testConnection = embeddedPostgres.getPostgresDatabase().getConnection();
        testConnection.setAutoCommit(false);

        // Create service that uses the same database
        service = new PostgreSQLSchedulingService() {
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
        if (embeddedPostgres != null) {
            embeddedPostgres.close();
        }
    }

    @Override
    protected PostgreSQLSchedulingService getService() {
        return service;
    }

    @Override
    protected void ensureConnectionOpen() throws SQLException {
        if (testConnection == null || testConnection.isClosed()) {
            testConnection = embeddedPostgres.getPostgresDatabase().getConnection();
            testConnection.setAutoCommit(false);
        }
    }

    @Override
    protected String getSchemaName() {
        return "public";
    }

    @Override
    protected void verifyAllDatabaseObjectsExist() throws SQLException {
        ensureConnectionOpen();

        // Verify table exists
        assertTrue(tableExists("sz_follow_up_tasks"), "Table sz_follow_up_tasks should exist");

        // Verify indexes exist
        assertTrue(indexExists("sz_task_dup"), "Index sz_task_dup should exist");
        assertTrue(indexExists("sz_task_lease"), "Index sz_task_lease should exist");

        // Verify table structure
        verifyTableColumns();
    }

    @Override
    protected boolean tableExists(String tableName) throws SQLException {
        DatabaseMetaData meta = testConnection.getMetaData();
        try (ResultSet rs = meta.getTables(null, "public", tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    @Override
    protected boolean indexExists(String indexName) throws SQLException {
        String sql = "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND indexname=?";
        try (PreparedStatement pstmt = testConnection.prepareStatement(sql)) {
            pstmt.setString(1, indexName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
}
