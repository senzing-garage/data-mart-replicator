package com.senzing.datamart.reports;

import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.ProducesJson;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.Default;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzLoadedStats;
import com.senzing.datamart.reports.model.SzSourceLoadedStats;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.util.LoggingUtilities.formatStackTrace;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * Provides services for the Loaded Statistics reports.
 */
public interface LoadedStatsReportsService extends ReportsService {
    /**
     * The prefix path for entity size reports services.
     */
    String LOADED_STATS_PREFIX = REPORTS_PREFIX + "/loaded";

    /**
     * The endpoint for the loaded statistics report.
     */
    String LOADED_STATS_ENDPOINT = LOADED_STATS_PREFIX + "/";

    /**
     * The endpoint for the source-specific loaded statistics report.
     */
    String SOURCE_LOADED_STATS_ENDPOINT = LOADED_STATS_PREFIX + "/data-sources/{dataSourceCode}";

    /**
     * The endpoint for the entities loaded for a data source.
     */
    String SOURCE_LOADED_ENTITIES_ENDPOINT = SOURCE_LOADED_STATS_ENDPOINT + "/entities";

    /**
     * Exposes
     * {@link LoadedStatsReports#getLoadedStatistics(Connection, Set, Timers)} as a
     * REST/JSON service at {@link #LOADED_STATS_ENDPOINT}.
     * 
     * @param onlyLoaded Set to <code>true</code> to only consider data sources that
     *                   have loaded record, otherwise set this to
     *                   <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzLoadedStats} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(LOADED_STATS_PREFIX)
    @Path(LOADED_STATS_ENDPOINT)
    @ProducesJson
    default SzLoadedStats getLoadedStatistics(@Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            Set<String> dataSources = (onlyLoaded) ? null : this.getConfiguredDataSources(true);

            return LoadedStatsReports.getLoadedStatistics(conn, dataSources, timers);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ReportsServiceException(e);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            } else {
                throw new ReportsServiceException(e);
            }

        } finally {
            conn = close(conn);
        }
    }

    /**
     * Exposes
     * {@link LoadedStatsReports#getSourceLoadedStatistics(Connection, String, Timers)}
     * as a REST/JSON service at {@link #SOURCE_LOADED_STATS_ENDPOINT}.
     * 
     * @param dataSource The data source code for which the report is being
     *                   requested.
     * 
     * @return The {@link SzSourceLoadedStats} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(SOURCE_LOADED_STATS_ENDPOINT)
    @ProducesJson
    default SzSourceLoadedStats getSourceLoadedStatistics(@Param("dataSourceCode") String dataSource) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return LoadedStatsReports.getSourceLoadedStatistics(conn, dataSource, timers);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ReportsServiceException(e);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            } else {
                throw new ReportsServiceException(e);
            }

        } finally {
            conn = close(conn);
        }

    }

    /**
     * Exposes
     * {@link LoadedStatsReports#getEntityIdsForDataSource(Connection, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at {@link #SOURCE_LOADED_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code for for the entities being
     *                      requested.
     * @param entityIdBound The bound value for the entity ID's that will be
     *                      returned.
     * @param boundType     The {@link SzBoundType} that describes how to apply the
     *                      specified entity ID bound.
     * @param pageSize      The maximum number of entity ID's to return.
     * 
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * 
     * @return The {@link SzEntitiesPage} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(SOURCE_LOADED_ENTITIES_ENDPOINT)
    @Path(SOURCE_LOADED_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getEntityIdsForDataSource(@Param("dataSourceCode") String dataSource, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return LoadedStatsReports.getEntityIdsForDataSource(conn, dataSource, entityIdBound, boundType, pageSize,
                    sampleSize, timers);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ReportsServiceException(e);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            } else {
                throw new ReportsServiceException(e);
            }

        } finally {
            conn = close(conn);
        }

    }

}
