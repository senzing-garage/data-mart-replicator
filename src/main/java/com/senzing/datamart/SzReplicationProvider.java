package com.senzing.datamart;

import java.util.Map;

import com.senzing.datamart.model.SzReportKey;
import com.senzing.sql.ConnectionProvider;

import com.senzing.sdk.SzEnvironment;
import com.senzing.sql.DatabaseType;
import com.senzing.util.Quantified.Statistic;

/**
 * Provides an interface for data mart replication {@link 
 * com.senzing.listener.service.scheduling.TaskHandler} implementations
 * to access the context in which they are executing.
 */
public interface SzReplicationProvider {
    /**
     * Enumerates many of the common task actions that a task handler will encounter
     * for the data mart replicator.
     */
    enum TaskAction {
        /**
         * Refresh an entity.
         */
        REFRESH_ENTITY, 

        /**
         * Refresh a relationship.
         */
        REFRESH_RELATION,

        /**
         * Update the data source summary report.
         */
        UPDATE_DATA_SOURCE_SUMMARY, 

        /**
         * Update the cross-source summary report.
         */
        UPDATE_CROSS_SOURCE_SUMMARY,

        /**
         * Update the entity size breakdown report.
         */
        UPDATE_ENTITY_SIZE_BREAKDOWN,

        /**
         * Update the entity relation breakdown report.
         */
        UPDATE_ENTITY_RELATION_BREAKDOWN
    };

    /**
     * Waits until the replication service is ready for handling tasks for at most
     * the specified number of milliseconds. Specify a negative number of
     * milliseconds to wait indefinitely or zero (0) to simply check if ready with
     * no waiting.
     *
     * @param timeoutMillis The maximum number of milliseconds to wait for this task
     *                      handler to become ready, a negative number to wait
     *                      indefinitely, or zero (0) to simply poll without
     *                      waiting.
     *
     * @return {@link Boolean#TRUE} if ready to handle tasks, {@link Boolean#FALSE}
     *         if not yet ready, and <code>null</code> if due to some failure we
     *         will never be ready to handle tasks.
     *
     * @throws InterruptedException If interrupted while waiting.
     */
    Boolean waitUntilReady(long timeoutMillis) throws InterruptedException;

    /**
     * Gets the {@link SzEnvironment} for accessing the associated entity
     * repository.
     *
     * @return The {@link SzEnvironment} for accessing the associated entity
     *         repository.
     */
    SzEnvironment getSzEnvironment();

    /**
     * Gets the {@link ConnectionProvider} to use for connecting to the data mart
     * database.
     *
     * @return The {@link ConnectionProvider} to use for connecting to the data mart
     *         database.
     */
    ConnectionProvider getConnectionProvider();

    /**
     * Gets the {@link DatabaseType} to use for working with the data mart database.
     *
     * @return The {@link DatabaseType} to use for working with the data mart
     *         database.
     */
    DatabaseType getDatabaseType();

    /**
     * Ensures the specified report update is scheduled to occur at some later time.
     *
     * @param reportAction The task action for updating the report.
     * @param reportKey    The report key for the report statistic that should be
     *                     updated.
     */
    void scheduleReportFollowUp(String reportAction, SzReportKey reportKey);

    /**
     * Gets the {@link Map} of {@link Statistic} keys to {@link Number}
     * values describing the statistics for this instance.
     * 
     * @return The {@link Map} of {@link Statistic} keys to {@link Number}
     *         values describing the statistics for this instance.
     */
    Map<Statistic, Number> getStatistics();
}
