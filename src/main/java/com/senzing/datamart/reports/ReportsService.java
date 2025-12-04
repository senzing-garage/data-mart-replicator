package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.senzing.sdk.SzException;
import com.senzing.util.Timers;

/**
 * Base interface implemented by all datamart reports services.
 */
public interface ReportsService {
    /**
     * The prefix for all data mart reports.
     */
    String REPORTS_PREFIX = "/statistics";

    /**
     * Provides the JDBC {@link Connection} to use for the report.
     * 
     * @return The JDBC {@link Connection} to use for the report.
     * 
     * @throws SQLException If a failure occurs.
     */
    Connection getConnection() throws SQLException;

    /**
     * Gets the {@link Set} of {@link String} data source codes
     * for all configured data sources in the associated entity
     * repository (optionally excluding the default data sources).
     * 
     * @param excludeDefault <code>true</code> if the default 
     *                       data sources should be excluded, otherwise
     *                       <code>false</code>.
     * 
     * @return The {@link Set} of {@link String} data source codes
     *         for all configured data sources in the associated
     *         entity repository. 
     * 
     * @throws SzException If a failure occurs.
     */
    Set<String> getConfiguredDataSources(boolean excludeDefault) 
        throws SzException;

    /**
     * Gets the {@link Timers} instance for timing the current operation.
     * By default this returns <code>null</code> so that the operations
     * are not timed.  This is typically overridden to return a 
     * thread-local {@link Timers} instance.
     * 
     * @return The {@link Timers} instance to use for an operation, 
     *         or <code>null</code> if not timing the operation.
     */
    default Timers getTimers() {
        return null;
    }
}
