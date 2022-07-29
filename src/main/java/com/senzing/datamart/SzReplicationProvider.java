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
