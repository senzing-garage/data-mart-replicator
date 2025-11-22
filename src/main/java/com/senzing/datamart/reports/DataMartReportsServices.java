package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import com.senzing.sql.ConnectionProvider;

/**
 * Provides annotated services for the data mart reports.
 */
public class DataMartReportsServices implements EntitySizeReportsService {
    /**
     * The {@link ConnectionProvider} for this instance.
     */
    private ConnectionProvider connProvider = null;

    /**
     * Default constructor if overriding {@link #getConnection()}
     * or {@link #getConnectionProvider()} so that it is not provided
     * at the time of construction.
     */
    protected DataMartReportsServices() {
        this.connProvider = null;
    }

    /**
     * Constructs with the specified {@link ConnectionProvider} 
     * for obtaining the JDBC {@link Connection}.
     * 
     * @param connProvider THe {@link ConnectionProvider} to use.
     * 
     * @throws NullPointerException If the specified {@link ConnectionProvider}
     *                              is <code>null</code>.
     */
    public DataMartReportsServices(ConnectionProvider connProvider) 
    {
        Objects.requireNonNull(
            connProvider, "The connection provider cannot be null");
        this.connProvider = connProvider;
    }

    /**
     * Gets the {@link ConnectionProvider} for this instance.
     * 
     * @return The {@link ConnectionProvider} for this instance.
     */
    protected ConnectionProvider getConnectionProvider() {
        return this.connProvider;
    }

    /**
     * Overridden to get the {@link Connection} using the {@link ConnectionProvider}
     * provided at construction.
     * 
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        ConnectionProvider cp = this.getConnectionProvider();
        if (cp == null) {
            throw new SQLException("No ConnectionProvider has been set");
        }
        return cp.getConnection();
    }
}
