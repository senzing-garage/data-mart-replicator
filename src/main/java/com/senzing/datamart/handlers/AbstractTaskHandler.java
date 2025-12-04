package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.SzReportKey;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.listener.service.scheduling.TaskHandler;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.DatabaseType;
import com.senzing.text.TextUtilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

import static com.senzing.datamart.SzReplicationProvider.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides an abstract base class for Data Mart Replicator {@link TaskHandler}
 * implementations.
 */
public abstract class AbstractTaskHandler implements TaskHandler {
    /**
     * The maximum batch size to use for batch updates to avoid high memory
     * consumption.
     */
    protected static final int MAX_BATCH_SIZE = 1000;

    /**
     * The backing {@link SzReplicationProvider}.
     */
    private SzReplicationProvider replicationProvider = null;

    /**
     * The {@link TaskAction} that is supported by this instance.
     */
    private String supportedAction = null;

    /**
     * Constructs with the specified {@link SzReplicationProvider} to back this task
     * handler and the supported {@link TaskAction}.
     *
     * @param provider        The {@link SzReplicationProvider} to use.
     * @param supportedAction The first supported {@link TaskAction}.
     */
    protected AbstractTaskHandler(SzReplicationProvider provider, TaskAction supportedAction) {
        Objects.requireNonNull(provider, "The SzReplicationProvider cannot be null");
        Objects.requireNonNull(supportedAction, "The supported task action cannot be null");

        this.replicationProvider = provider;
        this.supportedAction = supportedAction.toString();
    }

    /**
     * Gets the supported task action for this instance.
     *
     * @return The supported task action for this instance.
     */
    protected String getSupportedAction() {
        return this.supportedAction;
    }

    /**
     * Gets the {@link SzEnvironment} from the backing
     * {@link SzReplicationProvider}.
     * 
     * @return The {@link SzEnvironment} from the backing
     *         {@link SzReplicationProvider}.
     */
    protected SzEnvironment getSzEnvironment() {
        return this.replicationProvider.getSzEnvironment();
    }

    /**
     * Gets the {@link ConnectionProvider} from the backing
     * {@link SzReplicationProvider}.
     *
     * @return The {@link ConnectionProvider} from the backing
     *         {@link SzReplicationProvider}.
     */
    protected ConnectionProvider getConnectionProvider() {
        return this.replicationProvider.getConnectionProvider();
    }

    /**
     * Gets a JDBC {@link Connection} to use. The caller should close the
     * {@link Connection} to release it back to the pool for others to use.
     *
     * @return The {@link Connection} that was obtained.
     *
     * @throws SQLException If a JDBC failure occurs.
     */
    protected Connection getConnection() throws SQLException {
        ConnectionProvider provider = this.getConnectionProvider();
        return provider.getConnection();
    }

    /**
     * Gets the {@link DatabaseType} from the backing {@link SzReplicationProvider}.
     *
     * @return The {@link DatabaseType} from the backing
     *         {@link SzReplicationProvider}.
     */
    protected DatabaseType getDatabaseType() {
        return this.replicationProvider.getDatabaseType();
    }

    /**
     * Ensures the specified report update is scheduled to occur at some later time.
     *
     * @param reportAction The task action for updating the report.
     * @param reportKey    The report key for the report statistic that should be
     *                     updated.
     */
    protected void scheduleReportFollowUp(String reportAction, SzReportKey reportKey) {
        this.replicationProvider.scheduleReportFollowUp(reportAction, reportKey);
    }

    /**
     * Ensures the specified report update is scheduled to occur at some later time.
     *
     * @param reportAction The {@link SzReplicationProvider.TaskAction} for updating
     *                     the report.
     * @param reportKey    The report key for the report statistic that should be
     *                     updated.
     */
    protected void scheduleReportFollowUp(TaskAction reportAction, SzReportKey reportKey) {
        this.scheduleReportFollowUp(reportAction.toString(), reportKey);
    }

    /**
     * Implemented to return the result from calling
     * {@link SzReplicationProvider#waitUntilReady(long)} on the underlying
     * {@link SzReplicationProvider}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Boolean waitUntilReady(long timeoutMillis) throws InterruptedException {
        return this.replicationProvider.waitUntilReady(timeoutMillis);
    }

    /**
     * Implemented to check if the specified action is the support action and if not
     * throws an {@link IllegalArgumentException}. If it is the supported action
     * then {@link #handleTask(Map, int, Scheduler)} is called.
     *
     * {@inheritDoc}
     */
    @Override
    public void handleTask(String               action,
                           Map<String, Object>  parameters,
                           int                  multiplicity,
                           Scheduler            followUpScheduler)
        throws ServiceExecutionException 
    {
        logDebug("HANDLING TASK: " + action + " ( x " + multiplicity + " )", 
                 "WITH PARAMETERS: " + parameters);

        Objects.requireNonNull(action, "The action cannot be null");
        if (!action.equals(this.getSupportedAction())) {
            throw new IllegalArgumentException(
                "This TaskHandler implementation cannot handle the specified "
                    + "action.  action=[ " + action + " ], supported=[ "
                    + this.getSupportedAction() + " ]");
        }
        this.handleTask(parameters, multiplicity, followUpScheduler);
    }

    /**
     * THis method must be implemented to handle the supported task.
     *
     * @param parameters        The {@link Map} of {@link String} keys to
     *                          {@link Object} values for the task parameters.
     * @param multiplicity      The multiplicity for the task.
     * @param followUpScheduler The follow-up scheduler for the task.
     * @throws ServiceExecutionException If a failure occurs.
     */
    protected abstract void handleTask(Map<String, Object>  parameters, 
                                       int                  multiplicity,
                                       Scheduler            followUpScheduler)
        throws ServiceExecutionException;

    /**
     * Creates a virtually unique lease ID.
     *
     * @param prefixes The optional prefixes to put on the ID.
     *
     * @return A new lease ID to use.
     */
    protected String generateOperationId(Object... prefixes) {
        StringBuilder sb = new StringBuilder();

        // handle the prefixes
        if (prefixes.length > 0) {
            sb.append("[");
        }
        String sep = "";
        for (Object prefix : prefixes) {
            sb.append(sep).append(prefix);
            sep = ",";
        }
        if (prefixes.length > 0) {
            sb.append("]");
        }

        long pid = ProcessHandle.current().pid();
        sb.append("pid=").append(pid).append("|");
        sb.append(Instant.now().toString()).append("|");
        sb.append(TextUtilities.randomAlphanumericText(50));
        return sb.toString();
    }

    /**
     * An interface for binding a data value to a {@link PreparedStatement} and
     * optionally returning the number of rows expected to be updated or selected
     * for the bound statement.
     *
     * @param <T> The type of the object that holds the values to be bound.
     */
    protected interface Binder<T> {
        /**
         * Binds the properties of the specified value to the specified
         * {@link PreparedStatement}. This method optionally returns the expected number
         * of rows or the exact expected number of rows. If <code>null</code> is
         * returned then no exception is made on the returned number of rows. If a
         * non-negative number is returned then that exact number of rows is expected to
         * be updated/selected. If a negative number is returned then the absolute value
         * of that return value is an upper bound for the maximum number of rows to be
         * updated/selected.
         *
         * @param ps    The {@link PreparedStatement} to bind to.
         * @param value The value that holds the properties to be bound.
         * @return The number of expected rows to be returned when executing a query or
         *         the number of expected rows to be updated if executing an update as a
         *         non-negative value denoting an exact number and as a negative number
         *         denoting an upper-bound for the absolute value, or <code>null</code>
         *         if there is no expectation on the number of rows.
         * @throws SQLException If a failure occurs.
         */
        Integer bind(PreparedStatement ps, T value) throws SQLException;
    }

    /**
     * Binds the {@link Collection} of values to the specified
     * {@link PreparedStatement} as a batch update, executes the batch and verifies
     * the number of updated rows according to the return value from
     * {@link Binder#bind(PreparedStatement, Object)} for each respective value.
     * This method will cap the batch size at {@link #MAX_BATCH_SIZE}, execute the
     * batch and start a new batch repeatedly until all updates have been performed.
     *
     * @param ps     The {@link PreparedStatement} to bind.
     * @param binder The {@link Binder} to use for binding to the
     *               {@link PreparedStatement}.
     * @param data   The {@link Collection} of data values to bind.
     * @return The {@link List} of row counts for the updated rows, corresponding in
     *         iteration order to the specified {@link Collection} of data values
     *         for which the row count applies.
     * @param <T> The type of the objects being used to bind the
     *            {@link PreparedStatement}
     * @throws SQLException If a JDBC failure occurs.
     */
    protected <T> List<Integer> batchUpdate(PreparedStatement   ps, 
                                            Collection<T>       data,
                                            Binder<T>           binder) 
        throws SQLException 
    {
        int batchCount = 0;
        List<Integer> rowCounts = new ArrayList<>(data.size());
        List<Integer> expectedCounts = new ArrayList<>(data.size());
        for (T value : data) {
            expectedCounts.add(binder.bind(ps, value));
            ps.addBatch();
            batchCount++;
            // if we exceed the maximum batch size then execute early
            if (batchCount > MAX_BATCH_SIZE) {
                for (int rowCount : ps.executeBatch()) {
                    rowCounts.add(rowCount);
                }
                batchCount = 0;
            }
        }

        // execute anything remaining in the batch
        if (batchCount > 0) {
            int[] rowCountArray = ps.executeBatch();

            for (int rowCount : rowCountArray) {
                rowCounts.add(rowCount);
            }
        }

        // now check the results for number of rows updated
        int index = 0;
        int errorCount = 0;
        StringBuilder sb = new StringBuilder();
        String prefix = "";
        for (T value : data) {
            // get the expected row count
            Integer expectedRowCount = expectedCounts.get(index);

            // check if no expectation
            if (expectedRowCount == null) {
                continue;
            }

            // check if the expectation is an exact count
            boolean exact = (expectedRowCount >= 0);

            // if not exact then convert to an upper bound
            if (!exact) {
                expectedRowCount = -1 * expectedRowCount;
            }

            // get the actual row count
            int actualRowCount = rowCounts.get(index);

            // check the actual row count versus the expected row count
            if ((exact && (actualRowCount != expectedRowCount)) 
                || (!exact && (actualRowCount > expectedRowCount))) 
            {
                sb.append(prefix).append("{ [ expected=[ ");
                sb.append((exact) ? String.valueOf(expectedRowCount) 
                          : ("[0, " + expectedRowCount + "]"));
                sb.append(" ], actual=[ " + actualRowCount 
                          + " ], updatedValue=[ " + value + " ] }");
                prefix = ", ";
                errorCount++;
            }
        }

        // check if any errors
        if (errorCount > 0) {
            throw new IllegalStateException(
                    "Updated the wrong number of rows for " + errorCount + " of " 
                    + rowCounts.size() + " batched updates.  statement=[ " + ps 
                    + " ], failures=[ " + sb.toString() + " ]");
        }

        // return the row counts
        return rowCounts;
    }

    /**
     * Returns the summation of the values in the specified {@link Collection}.
     *
     * @param values The values to sum.
     * @return The sum of the values.
     */
    protected static int sum(Collection<Integer> values) {
        int result = 0;
        for (Integer value : values) {
            result += value.intValue();
        }
        return result;
    }
}
