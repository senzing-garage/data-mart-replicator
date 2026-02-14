package com.senzing.datamart;

import com.senzing.datamart.SzReplicationProvider.TaskAction;
import com.senzing.datamart.handlers.*;
import com.senzing.datamart.model.SzReportCode;
import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.schema.PostgreSQLSchemaBuilder;
import com.senzing.datamart.schema.SQLiteSchemaBuilder;
import com.senzing.datamart.schema.SchemaBuilder;
import com.senzing.listener.service.AbstractListenerService;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.listener.service.scheduling.SchedulingService;
import com.senzing.listener.service.scheduling.TaskHandler;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.DatabaseType;

import javax.json.JsonObject;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.senzing.listener.service.AbstractListenerService.MessagePart.*;
import static com.senzing.listener.service.ServiceUtilities.*;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.sql.DatabaseType.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Extends {@link AbstractListenerService} to implement a data mart replication.
 */
public class SzReplicatorService extends AbstractListenerService {
    /**
     * Constant for converting between nanoseconds and milliseconds.
     */
    private static final long ONE_MILLION = 1000000L;

    /**
     * The initialization key for obtaining the {@link ConnectionProvider} from the
     * {@link ConnectionProvider#REGISTRY}. This initialization parameter is
     * required.
     */
    public static final String CONNECTION_PROVIDER_KEY = "connectionProvider";

    /**
     * The initialization parameter key to specify the number of <b>seconds</b> to
     * wait before scheduling report update tasks as follow-up tasks.
     */
    public static final String REPORT_UPDATE_PERIOD_KEY = "reportUpdatePeriod";

    /**
     * The default wait period for periodically scheduling required report update
     * tasks (in seconds).
     */
    public static final long DEFAULT_REPORT_UPDATE_PERIOD = 60L;

    /**
     * The {@link Map} of {@link DatabaseType} keys to {@link SchemaBuilder} values.
     */
    private static final Map<DatabaseType, SchemaBuilder> SCHEMA_BUILDER_MAP 
        = Map.of(SQLITE, new SQLiteSchemaBuilder(),
                 POSTGRESQL, new PostgreSQLSchemaBuilder());

    /**
     * The {@link SzReplicationProvider} implementation used by
     * {@link SzReplicatorService}.
     */
    protected class Provider implements SzReplicationProvider {
        /**
         * Default constructor.
         */
        public Provider() {
            // do nothing
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Boolean waitUntilReady(long timeoutMillis) throws InterruptedException {
            return SzReplicatorService.this.waitUntilReady(timeoutMillis);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SzEnvironment getSzEnvironment() {
            return SzReplicatorService.this.getSzEnvironment();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ConnectionProvider getConnectionProvider() {
            return SzReplicatorService.this.getConnectionProvider();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DatabaseType getDatabaseType() {
            return SzReplicatorService.this.getDatabaseType();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void scheduleReportFollowUp(String reportAction, SzReportKey reportKey) {
            SzReplicatorService.this.lastReportActivityNanoTime.set(System.nanoTime());
            SzReplicatorService.this.reportUpdater.addReportTask(reportKey, reportAction);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Map<Statistic, Number> getStatistics() {
            return SzReplicatorService.this.getStatistics();
        }
    }

    /**
     * The {@link SzEnvironment} to use for working with Senzing.
     */
    private SzEnvironment environment;

    /**
     * The {@link DatabaseType} for this instance.
     */
    private DatabaseType databaseType;

    /**
     * The {@link ConnectionProvider} for accessing the database.
     */
    private ConnectionProvider connectionProvider = null;

    /**
     * The {@link ReportUpdater} to handle background report updates.
     */
    private ReportUpdater reportUpdater = null;

    /**
     * Tracks the time of the last report activity.
     */
    private AtomicLong lastReportActivityNanoTime = new AtomicLong(System.nanoTime());

    /**
     * The {@link SzReplicationProvider} for this instance.
     */
    private SzReplicationProvider provider = new Provider();

    /**
     * The {@link Map} of {@link TaskAction} action name keys to {@link TaskHandler}
     * values.
     */
    private Map<TaskAction, TaskHandler> handlerMap;

    /**
     * Background thread to handle periodically scheduling tasks to avoid having to
     * use follow-up tasks to increase performance.
     */
    public static class ReportUpdater extends Thread {
        /**
         * Flag indicating if this thread should shutdown.
         */
        private boolean shutdown = false;

        /**
         * The period between scheduling tasks (in seconds).
         */
        private long period = 0L;

        /**
         * The {@link Map} of {@link String} report keys to {@link String} task action
         * values for reports that need to be handled.
         */
        private Map<SzReportKey, String> reportKeyMap;

        /**
         * The parent {@link SzReplicatorService} for this instance.
         */
        private SzReplicatorService replicator = null;

        /**
         * Constructs with the specified period in milliseconds.
         *
         * @param replicator The {@link SzReplicatorService} that owns the report
         *                   updater.
         * @param period     The period for scheduling periodic tasks.
         */
        protected ReportUpdater(SzReplicatorService replicator, long period) {
            this.replicator = replicator;
            this.period = period;
            this.reportKeyMap = new LinkedHashMap<>();
        }

        /**
         * Checks if this flag is shutdown.
         *
         * @return <code>true</code> if shutdown, otherwise <code>false</code>.
         */
        public synchronized boolean isShutdown() {
            return this.shutdown;
        }

        /**
         * Triggers the shutdown of this thread.
         */
        protected synchronized void shutdown() {
            this.shutdown = true;
            this.notifyAll();
        }

        /**
         * Adds a report task that needs to be followed-up on.
         *
         * @param reportKey    The report key for the report that needs an update.
         * @param reportAction The {@link String} report action.
         */
        public synchronized void addReportTask(SzReportKey reportKey, String reportAction) {
            this.reportKeyMap.put(reportKey, reportAction);
        }

        /**
         * Periodically schedules tasks.
         */
        @Override
        public void run() {
            SchedulingService scheduling = this.replicator.getSchedulingService();

            while (!this.isShutdown()) {
                synchronized (this) {
                    Scheduler scheduler = (this.reportKeyMap.size() > 0)
                        ? scheduling.createScheduler(true) : null;
                    this.reportKeyMap.forEach((reportKey, action) -> {
                        replicator.lastReportActivityNanoTime.set(System.nanoTime());
                        scheduler.createTaskBuilder(action)
                                .resource("REPORT", reportKey.toString())
                                .parameter("reportKey", reportKey.toString()).schedule();
                    });
                    try {
                        if (scheduler != null) {
                            scheduler.commit();
                            this.reportKeyMap.clear();
                        }
                        
                    } catch (ServiceExecutionException e) {
                        logWarning(e, "FAILED TO SCHEDULE PERIODIC REPORT UPDATE: ");
                    }

                    try {
                        this.wait(this.period * 1000L);
                    } catch (InterruptedException ignore) {
                        // do nothing
                    }
                }
            }
        }
    }

    /**
     * Constructs with the specified {@link SzEnvironment}.
     * 
     * @param env The {@link SzEnvironment} to use.
     */
    public SzReplicatorService(SzEnvironment env) {
        super(Map.of(AFFECTED_ENTITY, REFRESH_ENTITY.toString()));

        // set the environment
        this.environment = env;

        RefreshEntityHandler entityHandler = new RefreshEntityHandler(this.provider);

        SourceSummaryReportHandler summaryHandler = new SourceSummaryReportHandler(this.provider);

        CrossSummaryReportHandler crossHandler = new CrossSummaryReportHandler(this.provider);

        SizeBreakdownReportHandler sizeBreakdownHandler = new SizeBreakdownReportHandler(this.provider);

        RelationBreakdownReportHandler relBreakdownHandler = new RelationBreakdownReportHandler(this.provider);

        this.handlerMap = Map.of(
            REFRESH_ENTITY, entityHandler,
            UPDATE_DATA_SOURCE_SUMMARY, summaryHandler,
            UPDATE_CROSS_SOURCE_SUMMARY, crossHandler,
            UPDATE_ENTITY_SIZE_BREAKDOWN, sizeBreakdownHandler,
            UPDATE_ENTITY_RELATION_BREAKDOWN, relBreakdownHandler);
    }

    /**
     * Gets the {@link SzReplicationProvider} for this instance.
     * 
     * @return The {@link SzReplicationProvider} for this instance.
     */
    public SzReplicationProvider getReplicationProvider() {
        return this.provider;
    }

    /**
     * Gets the initial pending report tasks.
     *
     * @return The {@link Map} of the {@link String} report keys to {@link String}
     *         report action values.
     *
     * @throws SQLException If a failure occurs.
     */
    protected Map<SzReportKey, String> getInitialReportTasks() throws SQLException {
        Map<SzReportKey, String> reportKeys = new LinkedHashMap<>();

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // get the connection
            conn = this.getConnection();

            // create a statement
            stmt = conn.createStatement();

            // get the pending report keys
            rs = stmt.executeQuery("SELECT report_key FROM sz_dm_pending_report");
            while (rs.next()) {
                String reportKeyText = rs.getString(1);
                SzReportKey reportKey = SzReportKey.parse(reportKeyText);

                SzReportCode reportCode = reportKey.getReportCode();
                String action = "UPDATE_" + reportCode;
                reportKeys.put(reportKey, action);
            }

            // return the report keys
            return reportKeys;

        } finally {
            rs = close(rs);
            stmt = close(stmt);
            conn = close(conn);
        }
    }

    /**
     * Gets the number of pending report updates.
     * 
     * @return The number of pending report updates.
     */
    protected long getPendingReportUpdateCount() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // get the connection
            conn = this.getConnection();

            // create a statement
            stmt = conn.createStatement();

            // get the pending report keys
            rs = stmt.executeQuery("SELECT COUNT(*) FROM sz_dm_pending_report");
            rs.next();

            synchronized (this.reportUpdater) {
                return ((long) this.reportUpdater.reportKeyMap.size())
                        + rs.getLong(1);
            }
        
        } catch (SQLException e) { 
            throw new IllegalStateException(e);

        } finally {
            rs = close(rs);
            stmt = close(stmt);
            conn = close(conn);
        }
    }

    /**
     * Checks if this replicator service has been idle for at least the
     * specified number of milliseconds, optionally waiting for the
     * specified maximum wait tine for it to become idle.
     * 
     * @param idleTime The number of milliseconds that the replicator
     *                 service must be idle before this will return
     *                 <code>true</code>.
     * 
     * @param maxWaitTime The maximum wait time in milliseconds to wait for
     *                    the replicator service to become idle, or zero (0)
     *                    if just checking without waiting and a negative
     *                    number to wait indefinitely.
     * 
     * @return <code>true</code> if idle, otherwise <code>false</code>.
     */
    public boolean waitUntilIdle(long idleTime, long maxWaitTime) {
        long    start           = System.nanoTime();
        long    maxWaitNanos    = maxWaitTime * ONE_MILLION;
        boolean firstPass       = true;
        do {
            // check if we should sleep before checking if idle
            if (!firstPass && maxWaitTime != 0L) {
                long now = System.nanoTime();
                long sleepTime = 1000L;
                if (maxWaitTime > 0L && ((maxWaitNanos - (now - start)) / ONE_MILLION < sleepTime)) {
                    sleepTime = (maxWaitNanos - (now - start)) / ONE_MILLION;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ignore) {
                    // do nothing
                }
            }

            firstPass = false; // ensure we wait on subsequent passes

            long updateCount    = this.getPendingReportUpdateCount();
            long taskCount      = this.getSchedulingService().getAllRemainingTasksCount();
            
            // check if nothing pending
            if (updateCount == 0L && taskCount == 0L) {
                // check the idle time
                long nowNanos       = System.nanoTime();
                long reportNanos    = this.lastReportActivityNanoTime.get();
                long taskNanos      = this.getSchedulingService().getLastTaskActivityNanoTime();

                // check if the scheduler and the report updates have been idle for long enough
                if ((((nowNanos - reportNanos) / ONE_MILLION) >= idleTime)
                    && (((nowNanos - taskNanos) / ONE_MILLION) >= idleTime))
                {
                    logInfo("SzReplicatorService found to be idle");
                    return true;
                }
            }
            
        } while (maxWaitTime < 0L || (maxWaitTime > 0L && (System.nanoTime() - start) < maxWaitNanos));
        logInfo("SzReplicatorService NOT found to be idle");
        return false;
    }

    /**
     * Implemented to delegate to a {@link TaskHandler} implementation based on the
     * specified action.
     *
     * @param action            The action for the task.
     * @param parameters        The parameters for the task.
     * @param multiplicity      The multiplier indicating how many times the task
     *                          was scheduled before being dispatched to be handled.
     * @param followUpScheduler The {@link Scheduler} for scheduling follow-up
     *                          tasks, or <code>null</code> if follow-up tasks
     *                          cannot be scheduled.
     * @throws ServiceExecutionException If a failure occurs.
     */
    @Override
    protected void handleTask(String                action, 
                              Map<String, Object>   parameters, 
                              int                   multiplicity,
                              Scheduler             followUpScheduler) 
        throws ServiceExecutionException 
    {
        TaskAction taskAction = null;
        try {
            taskAction = TaskAction.valueOf(action);

            TaskHandler handler = this.handlerMap.get(taskAction);

            if (handler == null) {
                throw new ServiceExecutionException(
                    "The specified action (" + action + ") is not recognized: " 
                    + this.handlerMap);
            }
            handler.handleTask(action, parameters, multiplicity, followUpScheduler);

        } catch (ServiceExecutionException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ServiceExecutionException(e);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected synchronized void doInit(JsonObject config) 
        throws ServiceSetupException 
    {
        try {
            // set up the connection provider
            String providerName = getConfigString(config, CONNECTION_PROVIDER_KEY, true);

            this.connectionProvider = ConnectionProvider.REGISTRY.lookup(providerName);

            this.databaseType = this.initDatabaseType();

            this.ensureSchema(false);

            long period = getConfigLong(config, 
                                        REPORT_UPDATE_PERIOD_KEY,
                                        1L,
                                        DEFAULT_REPORT_UPDATE_PERIOD);

            this.reportUpdater = new ReportUpdater(this, period);

            this.getInitialReportTasks().forEach((reportKey, action) -> {
                this.reportUpdater.addReportTask(reportKey, action);
            });

            this.reportUpdater.start();

        } catch (Exception e) {
            throw new ServiceSetupException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doDestroy() {
        ReportUpdater updater = null;
        synchronized (this) {
            updater = this.reportUpdater;
        }
        if (updater != null) {
            updater.shutdown();
        }

        try {
            if (updater != Thread.currentThread() && updater.isAlive())
            {
                updater.join();
            }

            synchronized (this) {
                if (updater == this.reportUpdater) {
                    this.reportUpdater = null;
                }
            }

        } catch (InterruptedException ignore) {
            // ignore
        }
    }

    /**
     * Initializes the {@link DatabaseType} to use for formatting SQL statements.
     *
     * @return The {@link DatabaseType} to use.
     *
     * @throws SQLException If a failure occurs.
     */
    protected DatabaseType initDatabaseType() throws SQLException {
        Connection conn = this.getConnection();
        try {
            return DatabaseType.detect(conn);
        } finally {
            conn = close(conn);
        }
    }

    /**
     * Gets the {@link DatabaseType} used by this instance.
     *
     * @return The {@link DatabaseType} used by this instance.
     */
    public DatabaseType getDatabaseType() {
        return this.databaseType;
    }

    /**
     * Ensures the schema exists and optionally drops it and recreates it.
     *
     * @param recreate <code>true</code> if the schema should be dropped and
     *                 recreated, or <code>false</code> if any existing schema
     *                 should be left in place.
     *
     * @throws SQLException If a JDBC failure occurs.
     */
    protected void ensureSchema(boolean recreate) throws SQLException {
        Connection conn = this.getConnection();
        try {
            // get the database type
            DatabaseType dbType = this.getDatabaseType();
            SchemaBuilder schemaBuilder = SCHEMA_BUILDER_MAP.get(dbType);
            if (schemaBuilder == null) {
                throw new SQLException("The database type is not supported.  databaseType=[ " + databaseType
                        + " ], productName=[ " + conn.getMetaData().getDatabaseProductName() + " ]");
            }
            schemaBuilder.ensureSchema(conn, recreate);

        } finally {
            conn = close(conn);
        }
    }

    /**
     * Gets the {@link ConnectionProvider} for this instance.
     *
     * @return The {@link ConnectionProvider} for this instance.
     */
    public ConnectionProvider getConnectionProvider() {
        return this.connectionProvider;
    }

    /**
     * Gets a new {@link Connection} from the backing {@link ConnectionProvider}.
     * When done with the {@link Connection} simply close it.
     *
     * @return The {@link Connection} from the backing {@link ConnectionProvider}.
     * @throws SQLException If a JDBC failure occurs.
     */
    protected Connection getConnection() throws SQLException {
        return this.getConnectionProvider().getConnection();
    }

    /**
     * Gets the backing {@link SzEnvironment} for this instance.
     *
     * @return The backing {@link SzEnvironment} for this instance.
     */
    protected SzEnvironment getSzEnvironment() {
        return this.environment;
    }
}
