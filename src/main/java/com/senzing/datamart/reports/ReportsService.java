package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.SQLException;

import com.senzing.util.Timers;

/**
 * Base interface implemented by all datamart reports services.
 */
public interface ReportsService {
    /**
     * Provides the JDBC {@link Connection} to use for the report.
     * 
     * @return The JDBC {@link Connection} to use for the report.
     * 
     * @throws SQLException If a failure occurs.
     */
    Connection getConnection() throws SQLException;

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
