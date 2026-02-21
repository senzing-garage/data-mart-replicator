package com.senzing.listener.communication.sql;

import com.senzing.sql.Connector;
import com.senzing.sql.SQLiteConnector;

import java.io.File;

/**
 * Unit tests for {@link SQLConsumer} using SQLite with a temporary file-based database.
 */
class SQLiteConsumerTest extends AbstractSQLConsumerTest {

    private File tempDbFile;

    @Override
    protected Connector createConnector() throws Exception {
        // Create a temporary database file
        tempDbFile = File.createTempFile("sqlite_consumer_test_", ".db");
        tempDbFile.deleteOnExit();
        return new SQLiteConnector(tempDbFile.getAbsolutePath());
    }

    @Override
    protected SQLClient createSQLClient() {
        return new SQLiteClient();
    }

    @Override
    protected void cleanupDatabase() throws Exception {
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    @Override
    protected String getProviderName() {
        return "sqlite-test-provider-" + System.currentTimeMillis();
    }
}
