package com.senzing.datamart;

import com.senzing.datamart.model.SzReportKey;
import com.senzing.listener.service.g2.G2Service;
import com.senzing.sql.ConnectionProvider;

import com.senzing.listener.service.scheduling.TaskHandler;
import com.senzing.sql.DatabaseType;

/**
 * Provides an interface for data mart replication {@link TaskHandler}
 * implementations to access the context in which they are executing.
 */
public interface SzReplicationProvider {
  /**
   * Enumerates many of the common task actions that a task handler will
   * encounter for the data mart replicator.
   */
  enum TaskAction {
    REFRESH_ENTITY,
    REFRESH_RELATION,
    UPDATE_DATA_SOURCE_SUMMARY,
    UPDATE_CROSS_SOURCE_SUMMARY,
    UPDATE_ENTITY_SIZE_BREAKDOWN,
    UPDATE_ENTITY_RELATION_BREAKDOWN
  };

  /**
   * Waits until the replication service is ready for handling tasks for at
   * most the specified number of milliseconds.  Specify a negative number of
   * milliseconds to wait indefinitely or zero (0) to simply check if ready
   * with no waiting.
   *
   * @param timeoutMillis The maximum number of milliseconds to wait for this
   *                      task handler to become ready, a negative number to
   *                      wait indefinitely, or zero (0) to simply poll without
   *                      waiting.
   *
   * @return {@link Boolean#TRUE} if ready to handle tasks, {@link
   *         Boolean#FALSE} if not yet ready, and <code>null</code> if due to
   *         some failure we will never be ready to handle tasks.
   *
   * @throws InterruptedException If interrupted while waiting.
   */
  Boolean waitUntilReady(long timeoutMillis) throws InterruptedException;

  /**
   * Gets the {@link G2Service} for accessing the associated entity repository.
   *
   * @return The {@link G2Service} for accessing the associated entity
   *         repository.
   */
  G2Service getG2Service();

  /**
   * Gets the {@link ConnectionProvider} to use for connecting to the
   * data mart database.
   *
   * @return The {@link ConnectionProvider} to use for connecting to the
   *         data mart database.
   */
  ConnectionProvider getConnectionProvider();

  /**
   * Gets the {@link DatabaseType} to use for working with the data mart
   * database.
   *
   * @return The {@link DatabaseType} to use for working with the data mart
   *         database.
   */
  DatabaseType getDatabaseType();

  /**
   * Ensures the specified report update is scheduled to occur at some later
   * time.
   *
   * @param reportAction The task action for updating the report.
   * @param reportKey The report key for the report statistic that should be
   *                  updated.
   */
  void scheduleReportFollowUp(String reportAction, SzReportKey reportKey);
}
