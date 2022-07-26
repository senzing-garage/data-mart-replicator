package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.SzReportKey;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.g2.G2Service;
import com.senzing.listener.service.scheduling.Scheduler;
import com.senzing.listener.service.scheduling.TaskHandler;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import static com.senzing.datamart.SzReplicationProvider.*;

/**
 * Provides an abstract base class for Data Mart Replicator {@link TaskHandler}
 * implementations.
 */
public abstract class AbstractTaskHandler implements TaskHandler {
  /**
   * The backing {@link SzReplicationProvider}.
   */
  private SzReplicationProvider replicationProvider = null;

  /**
   * The {@link TaskAction} that is supported by this instance.
   */
  private String supportedAction = null;

  /**
   * Constructs with the specified {@Link SzReplicationProvider} to back this
   * task handler and the supported {@link TaskAction}.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   * @param supportedAction The first supported {@link TaskAction}.
   */
  protected AbstractTaskHandler(SzReplicationProvider provider,
                                TaskAction            supportedAction)
  {
    Objects.requireNonNull(
        provider, "The SzReplicationProvider cannot be null");
    Objects.requireNonNull(
        supportedAction, "The supported task action cannot be null");

    this.replicationProvider  = provider;
    this.supportedAction      = supportedAction.toString();
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
   * Gets the {@link G2Service} from the backing {@link SzReplicationProvider}.
   *
   * @return The {@link G2Service} from the backing {@link
   * SzReplicationProvider}.
   */
  protected G2Service getG2Service() {
    return this.replicationProvider.getG2Service();
  }

  /**
   * Gets the {@link ConnectionProvider} from the backing {@link
   * SzReplicationProvider}.
   *
   * @return The {@link ConnectionProvider} from the backing {@link
   * SzReplicationProvider}.
   */
  protected ConnectionProvider getConnectionProvider() {
    return this.replicationProvider.getConnectionProvider();
  }

  /**
   * Gets a JDBC {@link Connection}
   */
  protected Connection getConnection() throws SQLException {
    return this.getConnectionProvider().getConnection();
  }

  /**
   * Gets the {@link DatabaseType} from the backing {@link
   * SzReplicationProvider}.
   *
   * @return The {@link DatabaseType} from the backing {@link
   * SzReplicationProvider}.
   */
  protected DatabaseType getDatabaseType() {
    return this.replicationProvider.getDatabaseType();
  }

  /**
   * Ensures the specified report update is scheduled to occur at some later
   * time.
   *
   * @param reportAction The task action for updating the report.
   * @param reportKey    The report key for the report statistic that should be
   *                     updated.
   */
  protected void scheduleReportFollowUp(String      reportAction,
                                        SzReportKey reportKey) {
    this.replicationProvider.scheduleReportFollowUp(reportAction, reportKey);
  }

  /**
   * Ensures the specified report update is scheduled to occur at some later
   * time.
   *
   * @param reportAction The {@link SzReplicationProvider.TaskAction} for
   *                     updating the report.
   * @param reportKey    The report key for the report statistic that should be
   *                     updated.
   */
  protected void scheduleReportFollowUp(TaskAction  reportAction,
                                        SzReportKey reportKey)
  {
    this.scheduleReportFollowUp(reportAction.toString(), reportKey);
  }

  /**
   * Implemented to check if the specified action is the support action and if
   * not throws an {@link IllegalArgumentException}.  If it is the supported
   * action then {@link #handleTask(Map, int, Scheduler)} is called.
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
   * @param parameters The {@link Map} of {@link String} keys to {@link Object}
   *                   values for the task parameters.
   * @param multiplicity The multiplicity for the task.
   * @param followUpScheduler The follow-up scheduler for the task.
   */
  protected abstract void handleTask(Map<String, Object>  parameters,
                                     int                  multiplicity,
                                     Scheduler            followUpScheduler)
      throws ServiceExecutionException;
}
