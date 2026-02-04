package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.sdk.SzException;
import com.senzing.sql.ConnectionProvider;

/**
 * Abstract test class that implements {@link ReportsService} for use
 * in unit tests. Obtains connections and data source information from
 * a {@link Repository} instance.
 */
public abstract class AbstractTestReportsService implements ReportsService {

    /**
     * The default data sources to exclude when {@code excludeDefault} is true.
     */
    private static final Set<String> DEFAULT_DATA_SOURCES = Set.of("TEST", "SEARCH");

    /**
     * The {@link Repository} providing access to the test data mart.
     */
    private final Repository repository;

    /**
     * The {@link ConnectionProvider} for obtaining database connections.
     */
    private final ConnectionProvider connectionProvider;

    /**
     * Constructs with the specified {@link ConnectionProvider} and
     * {@link Repository}.
     *
     * @param connectionProvider The {@link ConnectionProvider} to use for
     *                           obtaining database connections.
     * @param repository The {@link Repository} to use for obtaining
     *                   data source information.
     *
     * @throws NullPointerException If either parameter is {@code null}.
     */
    protected AbstractTestReportsService(ConnectionProvider  connectionProvider,
                                         Repository          repository)
    {
        Objects.requireNonNull(connectionProvider, "ConnectionProvider cannot be null");
        Objects.requireNonNull(repository, "Repository cannot be null");
        this.connectionProvider = connectionProvider;
        this.repository = repository;
    }

    /**
     * Gets the {@link Repository} for this instance.
     *
     * @return The {@link Repository} for this instance.
     */
    protected Repository getRepository() {
        return this.repository;
    }

    /**
     * Gets the {@link ConnectionProvider} for this instance.
     *
     * @return The {@link ConnectionProvider} for this instance.
     */
    protected ConnectionProvider getConnectionProvider() {
        return this.connectionProvider;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to obtain the connection from the underlying
     * {@link Repository}'s {@link ConnectionProvider}.
     */
    @Override
    public Connection getConnection() throws SQLException {
        return this.connectionProvider.getConnection();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to obtain the configured data sources from the
     * underlying {@link Repository}, optionally excluding the
     * default TEST and SEARCH data sources.
     */
    @Override
    public Set<String> getConfiguredDataSources(boolean excludeDefault)
        throws SzException
    {
        Set<String> dataSources = this.repository.getConfiguredDataSources();

        if (!excludeDefault) {
            return dataSources;
        }

        // Filter out the default data sources (TEST and SEARCH)
        Set<String> result = new TreeSet<>();
        for (String dataSource : dataSources) {
            if (!DEFAULT_DATA_SOURCES.contains(dataSource)) {
                result.add(dataSource);
            }
        }
        return result;
    }

    // getTimers() uses the default implementation from ReportsService
    // which returns null
}
