package com.senzing.datamart;

import com.senzing.datamart.handlers.*;
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

/**
 *
 */
public class SzReplicatorService extends AbstractListenerService {
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
   *
   */
  protected class Provider implements SzReplicationProvider {
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
      SzReplicatorService.this.reportUpdater.addReportTask(
          reportKey.toString(), reportAction);
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
    private Map<String, String> reportKeyMap;

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
    public synchronized void addReportTask(String reportKey,
                                           String reportAction)
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
                .resource("REPORT", reportKey)
                .parameter("reportKey", reportKey)
                .schedule();
          });
          try {
            scheduler.commit();
            this.reportKeyMap.clear();

          } catch (ServiceExecutionException e) {
            System.err.println();
            System.err.println("*******************************************");
            System.err.println("FAILED TO SCHEDULE PERIODIC REPORT UPDATE: ");
            e.printStackTrace();
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
  protected Map<String, String> getInitialReportTasks() throws SQLException {
    Map<String, String> reportKeys = new LinkedHashMap<>();

    Connection  conn  = null;
    Statement   stmt  = null;
    ResultSet   rs    = null;
    try {
      // create a statement
      stmt = conn.createStatement();

      // get the pending DSS report values
      rs = stmt.executeQuery("SELECT DISTINCT data_source1, data_source2 "
                                 + "FROM sz_pending_dss");
      while (rs.next()) {
        String dataSource1 = rs.getString(1);
        String dataSource2 = rs.getString(2);
        reportKeys.put(
            "DSS:" + dataSource1 + ":" + dataSource2, "UPDATE_DSS");
      }
      rs = close(rs);

      // get the pending CSS report values
      rs = stmt.executeQuery("SELECT DISTINCT data_source1, data_source2 "
                                 + "FROM sz_pending_css");
      while (rs.next()) {
        String dataSource1 = rs.getString(1);
        String dataSource2 = rs.getString(2);
        reportKeys.put(
            "CSS:" + dataSource1 + ":" + dataSource2, "UPDATE_CSS");
      }
      rs = close(rs);

      // get the pending ESB report values
      rs = stmt.executeQuery("SELECT DISTINCT entity_size "
                                 + "FROM sz_pending_esb");
      while (rs.next()) {
        int entitySize = rs.getInt(1);
        reportKeys.put("ESB:" + entitySize, "UPDATE_ESB");
      }
      rs = close(rs);
      stmt = close(stmt);

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
      String providerName = getConfigString(
          config, CONNECTION_PROVIDER_KEY, true);

      this.connectionProvider
          = ConnectionProvider.REGISTRY.lookup(providerName);

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

  @Override
  protected void doDestroy() {
    // do nothing
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
