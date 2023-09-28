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
import com.senzing.listener.service.g2.G2Service;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.listener.service.scheduling.SchedulingService;
import com.senzing.listener.service.scheduling.TaskHandler;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.DatabaseType;

import javax.json.JsonObject;
import java.sql.*;
import java.util.*;

import static com.senzing.listener.service.AbstractListenerService.MessagePart.*;
import static com.senzing.listener.service.ServiceUtilities.*;
import static com.senzing.datamart.SzReplicationProvider.*;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;
import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.sql.DatabaseType.*;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Extends {@link AbstractListenerService} to implement a data mart replication.
 */
public class SzReplicatorService extends AbstractListenerService {
  /**
   * The initialization parameter key for specifying the {@link JsonObject}
   * used to initialize the backing {@link G2Service}.  This parameter is
   * required.
   */
  public static final String G2_SERVICE_CONFIG_KEY = "g2ServiceConfig";

  /**
   * The initialization key for obtaining the {@link ConnectionProvider}
   * from the {@link ConnectionProvider#REGISTRY}.  This initialization
   * parameter is required.
   */
  public static final String CONNECTION_PROVIDER_KEY = "connectionProvider";

  /**
   * The initialization parameter key to specify the number of <b>seconds</b>
   * to wait before scheduling report update tasks as follow-up tasks.
   */
  public static final String REPORT_UPDATE_PERIOD_KEY = "reportUpdatePeriod";

  /**
   * The default wait period for periodically scheduling required report update
   * tasks (in seconds).
   */
  public static final long DEFAULT_REPORT_UPDATE_PERIOD = 60L;

  /**
   * The {@link Map} of {@link DatabaseType} keys to {@link SchemaBuilder}
   * values.
   */
  private static Map<DatabaseType, SchemaBuilder> SCHEMA_BUILDER_MAP
      = Map.of(SQLITE, new SQLiteSchemaBuilder(),
               POSTGRESQL, new PostgreSQLSchemaBuilder());

  /**
   * The {@link SzReplicationProvider} implementation used by {@link
   * SzReplicatorService}.
   */
  protected class Provider implements SzReplicationProvider {
    @Override
    public Boolean waitUntilReady(long timeoutMillis)
        throws InterruptedException
    {
      return SzReplicatorService.this.waitUntilReady(timeoutMillis);
    }

    @Override
    public G2Service getG2Service() {
      return SzReplicatorService.this.getG2Service();
    }

    @Override
    public ConnectionProvider getConnectionProvider() {
      return SzReplicatorService.this.getConnectionProvider();
    }

    @Override
    public DatabaseType getDatabaseType() {
      return SzReplicatorService.this.getDatabaseType();
    }

    @Override
    public void scheduleReportFollowUp(String       reportAction,
                                       SzReportKey  reportKey) {
      SzReplicatorService.this.reportUpdater.addReportTask(reportKey,
                                                           reportAction);
    }
  }

  /**
   * The {@link G2Service} to use for working with Senzing.
   */
  private G2Service g2Service;

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
   * The {@link SzReplicationProvider} for this instance.
   */
  private SzReplicationProvider provider = new Provider();

  /**
   * The {@link Map} of {@link TaskAction} action name keys to
   * {@link TaskHandler} values.
   */
  private Map<TaskAction, TaskHandler> handlerMap;

  /**
   * Background thread to handle periodically scheduling tasks to avoid having
   * to use follow-up tasks to increase performance.
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
     * The {@link Map} of {@link String} report keys to {@link String} task
     * action values for reports that need to be handled.
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
     * @param period The period for scheduling periodic tasks.
     */
    protected ReportUpdater(SzReplicatorService replicator, long period) {
      this.replicator   = replicator;
      this.period       = period;
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
     * @param reportKey The report key for the report that needs an update.
     * @param reportAction The {@link String} report action.
     */
    public synchronized void addReportTask(SzReportKey  reportKey,
                                           String       reportAction)
    {
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
          Scheduler scheduler = scheduling.createScheduler(true);
          this.reportKeyMap.forEach((reportKey, action) -> {
            scheduler.createTaskBuilder(action)
                .resource("REPORT", reportKey.toString())
                .parameter("reportKey", reportKey.toString())
                .schedule();
          });
          try {
            scheduler.commit();
            this.reportKeyMap.clear();

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
   * Default constructor.
   */
  public SzReplicatorService() {
    super(Map.of(AFFECTED_ENTITY, REFRESH_ENTITY.toString()));

    RefreshEntityHandler entityHandler
        = new RefreshEntityHandler(this.provider);

    SourceSummaryReportHandler summaryHandler
        = new SourceSummaryReportHandler(this.provider);

    CrossSummaryReportHandler crossHandler
        = new CrossSummaryReportHandler(this.provider);

    SizeBreakdownReportHandler sizeBreakdownHandler
        = new SizeBreakdownReportHandler(this.provider);

    RelationBreakdownReportHandler relBreakdownHandler
        = new RelationBreakdownReportHandler(this.provider);

    this.handlerMap = Map.of(
        REFRESH_ENTITY, entityHandler,
        UPDATE_DATA_SOURCE_SUMMARY, summaryHandler,
        UPDATE_CROSS_SOURCE_SUMMARY, crossHandler,
        UPDATE_ENTITY_SIZE_BREAKDOWN, sizeBreakdownHandler,
        UPDATE_ENTITY_RELATION_BREAKDOWN, relBreakdownHandler);
  }

  /**
   * Gets the initial pending report tasks.
   *
   * @return The {@link Map} of the {@link String} report keys to {@link String}
   *         report action values.
   *
   * @throws SQLException If a failure occurs.
   */
  protected Map<SzReportKey, String> getInitialReportTasks()
      throws SQLException
  {
    Map<SzReportKey, String> reportKeys = new LinkedHashMap<>();

    Connection  conn  = null;
    Statement   stmt  = null;
    ResultSet   rs    = null;
    try {
      // get the connection
      conn = this.getConnection();

      // create a statement
      stmt = conn.createStatement();

      // get the pending report keys
      rs = stmt.executeQuery("SELECT report_key FROM sz_dm_pending_report");
      while (rs.next()) {
        String        reportKeyText = rs.getString(1);
        SzReportKey   reportKey     = SzReportKey.parse(reportKeyText);

        SzReportCode  reportCode    = reportKey.getReportCode();
        String        action        = "UPDATE_" + reportCode;
        reportKeys.put(reportKey, action);
      }

      // return the report keys
      return reportKeys;

    } finally {
      rs    = close(rs);
      stmt  = close(stmt);
      conn  = close(conn);
    }
  }

  /**
   * Implemented to delegate to a {@link TaskHandler} implementation based
   * on the specified action.
   *
   * @param action The action for the task.
   * @param parameters The parameters for the task.
   * @param multiplicity The multiplier indicating how many times the task was
   *                     scheduled before being dispatched to be handled.
   * @param followUpScheduler The {@link Scheduler} for scheduling follow-up
   *                          tasks, or <code>null</code> if follow-up tasks
   *                          cannot be scheduled.
   * @throws ServiceExecutionException If a failure occurs.
   */
  @Override
  protected void handleTask(String              action,
                            Map<String, Object> parameters,
                            int                 multiplicity,
                            Scheduler           followUpScheduler)
      throws ServiceExecutionException
  {
    TaskAction taskAction = null;
    try {
      taskAction = TaskAction.valueOf(action);

      TaskHandler handler = this.handlerMap.get(taskAction);

      handler.handleTask(action, parameters, multiplicity, followUpScheduler);

    } catch (Exception e) {
      throw new ServiceExecutionException(
          "The specified action is not recognized: " + action);
    }

  }

  @Override
  protected void doInit(JsonObject config) throws ServiceSetupException {
    try {
      // get the G2 service config
      JsonObject g2ServiceConfig = getJsonObject(config, G2_SERVICE_CONFIG_KEY);
      if (g2ServiceConfig == null) {
        throw new ServiceSetupException(
            "The " + G2_SERVICE_CONFIG_KEY + " initialization parameter is "
            + "required, but is missing: " + toJsonText(config));
      }

      // initialize the G2 service
      this.g2Service = new G2Service();
      this.g2Service.init(g2ServiceConfig);

      String providerName = getConfigString(
          config, CONNECTION_PROVIDER_KEY, true);

      this.connectionProvider
          = ConnectionProvider.REGISTRY.lookup(providerName);

      this.databaseType = this.initDatabaseType();

      this.ensureSchema(false);

      Connection conn = this.getConnection();
      PreparedStatement ps 
        = conn.prepareStatement("SELECT COUNT(*) FROM sz_dm_pending_report");
      ResultSet rs = ps.executeQuery();
      int count = rs.getInt(1);
      System.out.println(" *********** (3) sz_dm_pending_report table created: "
                          + count);
      rs = close(rs);
      ps = close(ps);
      conn = close(conn);

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

  @Override
  protected void doDestroy() {
    this.reportUpdater.shutdown();
    try {
      this.reportUpdater.join();
    } catch (InterruptedException ignore) {
      // ignore
    }
    this.g2Service.destroy();
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
        throw new SQLException(
            "The database type is not supported.  databaseType=[ "
                + databaseType + " ], productName=[ "
                + conn.getMetaData().getDatabaseProductName() + " ]");
      }
      schemaBuilder.ensureSchema(conn, recreate);

    } finally {
      conn = close(conn);
    }
    conn = this.getConnection();
    PreparedStatement ps 
    = conn.prepareStatement("SELECT COUNT(*) FROM sz_dm_pending_report");
    ResultSet rs = ps.executeQuery();
    int count = rs.getInt(1);
    System.out.println(" *********** (2) sz_dm_pending_report table created: "
                        + count);
    rs = close(rs);
    ps = close(ps);
    conn = close(conn);

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
   * Gets the backing {@link G2Service} for this instance.
   *
   * @return The backing {@link G2Service} for this instance.
   */
  protected G2Service getG2Service() {
    return this.g2Service;
  }
}
