package com.senzing.listener.communication.sql;

import com.senzing.sql.Connector;
import com.senzing.sql.PostgreSqlConnector;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * Unit tests for {@link SQLConsumer} using PostgreSQL via Zonky Embedded PostgreSQL.
 */
class PostgreSQLConsumerTest extends AbstractSQLConsumerTest {

    private EmbeddedPostgres embeddedPostgres;

    @Override
    protected Connector createConnector() throws Exception {
        embeddedPostgres = EmbeddedPostgres.builder().start();
        int port = embeddedPostgres.getPort();
        return new PostgreSqlConnector("localhost", port, "postgres", "postgres", "postgres");
    }

    @Override
    protected SQLClient createSQLClient() {
        return new PostgreSQLClient();
    }

    @Override
    protected void cleanupDatabase() throws Exception {
        if (embeddedPostgres != null) {
            embeddedPostgres.close();
        }
    }

    @Override
    protected String getProviderName() {
        return "postgresql-test-provider-" + System.currentTimeMillis();
    }
}
