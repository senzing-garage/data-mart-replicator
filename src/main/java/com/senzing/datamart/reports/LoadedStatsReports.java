package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzLoadedStats;
import com.senzing.datamart.reports.model.SzSourceLoadedStats;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.model.SzReportStatistic.ENTITY_COUNT;
import static com.senzing.datamart.model.SzReportStatistic.UNMATCHED_COUNT;
import static com.senzing.datamart.reports.ReportUtilities.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides Loaded Statistics Report functionality.
 */
public final class LoadedStatsReports {
    /**
     * Private default constructor.
     */
    private LoadedStatsReports() {
        // do nothing
    }

    /**
     * Gets all the count stats including total record count, total entity count and
     * total unmatched record count along with a breakdown of record count, entity
     * count and unmatched record count by data source.
     * 
     * <p>
     * The statistics for data sources with loaded records are <b>ALWAYS</b> included
     * in the report.  However, a {@link Set} of {@link String} data source codes can
     * be specified for those data sources for which "zero" statistics should be included
     * even if no records are loaded for those data sources.  <b>NOTE:</b> this will
     * <b>NOT</b> filter statistics so that statistics for data sources with loaded
     * records can be excluded.
     *
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param dataSources The optional {@link Set} of {@link String} data source codes
     *                    for those data sources to be included in the statistics even
     *                    if they have no records loaded, or <code>null</code> if the
     *                    results should simply include statistics for data sources for
     *                    which records have been loaded.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzLoadedStats} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzLoadedStats getLoadedStatistics(Connection  conn,
                                                    Set<String> dataSources,
                                                    Timers      timers)
        throws NullPointerException, SQLException
    {
        Objects.requireNonNull(conn, "The connection cannot be null");

        PreparedStatement ps = null;
        ResultSet rs = null;
        SzLoadedStats result = new SzLoadedStats();
        Set<String> unusedDataSources 
            = (dataSources == null || dataSources.size() == 0)
                ? null : new TreeSet<>(dataSources);
        
        try {
            // get the total entity count
            queryingDatabase(timers, "selectEntityCount");
            try {
                
                // prepare the total entity count query
                ps = conn.prepareStatement(
                    "SELECT SUM(entity_count) FROM sz_dm_report WHERE report=?");

                // bind the report code
                ps.setString(1, ENTITY_SIZE_BREAKDOWN.getCode());

                // execute the query
                rs = ps.executeQuery();

                // read the results
                rs.next();
                result.setTotalEntityCount(rs.getLong(1));

            } finally {
                queriedDatabase(timers, "selectEntityCount");
                // release resources
                rs = close(rs);
                ps = close(ps);
            }

            // create a map to keep track of the source stats
            SortedMap<String, SzSourceLoadedStats> sourceStatsMap = new TreeMap<>();

            // now get the source entity and record counts
            long totalRecordCount = 0L;

            queryingDatabase(timers, "selectCountsBySource");
            try {
                // prepare the statement
                ps = conn.prepareStatement("SELECT data_source1, entity_count, record_count "
                        + "FROM sz_dm_report WHERE report=? AND statistic=? "
                        + "ORDER BY data_source1");

                // bind the report code and statistic
                ps.setString(1, DATA_SOURCE_SUMMARY.getCode());
                ps.setString(2, ENTITY_COUNT.toString());

                // execute the query
                rs = ps.executeQuery();

                // iterate over the query results
                while (rs.next()) {
                    String dataSource = rs.getString(1);
                    long entityCount = rs.getLong(2);
                    long recordCount = rs.getLong(3);

                    // increment the total record count
                    totalRecordCount += recordCount;

                    SzSourceLoadedStats stats = new SzSourceLoadedStats(dataSource);
                    stats.setEntityCount(entityCount);
                    stats.setRecordCount(recordCount);

                    sourceStatsMap.put(dataSource, stats);
                    if (unusedDataSources != null) {
                        unusedDataSources.remove(dataSource);
                    }
                }

            } finally {
                queriedDatabase(timers, "selectCountsBySource");
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // set the total record count
            result.setTotalRecordCount(totalRecordCount);

            long totalUnmatchedRecordCount = 0L;

            queryingDatabase(timers, "selectUnmatchedCountsBySource");
            try {
                // now get the counts by data source
                ps = conn.prepareStatement("SELECT data_source1, entity_count "
                        + "FROM sz_dm_report WHERE report=? AND statistic=? "
                        + "ORDER BY data_source1");

                // bind the parameters
                ps.setString(1, DATA_SOURCE_SUMMARY.getCode());
                ps.setString(2, UNMATCHED_COUNT.toString());

                // execute the query
                rs = ps.executeQuery();

                // iterate over the query results
                while (rs.next()) {
                    String dataSource = rs.getString(1);
                    long unmatchedCount = rs.getLong(2);

                    // increment the total record count
                    totalUnmatchedRecordCount += unmatchedCount;

                    SzSourceLoadedStats stats = sourceStatsMap.remove(dataSource);
                    if (stats == null) {
                        stats = new SzSourceLoadedStats(dataSource);
                        logWarning("Missing entity and record count stats for data source, "
                                   + "but got unmatched record count stats: " + dataSource);
                    }

                    // set the unmatched record count
                    stats.setUnmatchedRecordCount(unmatchedCount);

                    // add the source stats to the result
                    result.addDataSourceCount(stats);
                    if (unusedDataSources != null) {
                        unusedDataSources.remove(dataSource);
                    }
                }

            } finally {
                queriedDatabase(timers, "selectUnmatchedCountsBySource");
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // set the total unmatched record count
            result.setTotalUnmatchedRecordCount(totalUnmatchedRecordCount);

            if (unusedDataSources != null) {
                // iterate over the unused data sources
                unusedDataSources.forEach(dataSource -> {
                    SzSourceLoadedStats stats = new SzSourceLoadedStats(dataSource);
                    stats.setEntityCount(0);
                    stats.setRecordCount(0);
                    stats.setUnmatchedRecordCount(0);
                    sourceStatsMap.put(dataSource, stats);
                });
            }

            // add the source stats to the result
            sourceStatsMap.values().forEach(stats -> {
                if (!unusedDataSources.contains(stats.getDataSource())) {
                    logWarning("Missing unmatched record count stats for data source (" 
                                + stats.getDataSource() + "), but got entity and record "
                                + "count stats:  dataSources=[ " + dataSources 
                                + " ], unusedDataSources=[ " + unusedDataSources + " ]");
                }
                result.addDataSourceCount(stats);
            });

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets the {@link SzSourceLoadedStats} describing the record counts,
     * entity counts and unmatched records for a specific data source.
     * 
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param dataSource The non-null data source code identifying the data
     *                   source for which the count statistics are being requested.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzSourceLoadedStats} describing the record counts, 
     *         entity counts and unmatched record counts for the specified 
     *         data source.
     * 
     * @throws NullPointerException If either of the specified parameters is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzSourceLoadedStats getSourceLoadedStatistics(Connection  conn,
                                                                String      dataSource,
                                                                Timers      timers) 
        throws NullPointerException, SQLException
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        Objects.requireNonNull(dataSource, "The data source code cannot be null");
        
        // get the connection
        PreparedStatement ps = null;
        ResultSet rs = null;
        SzSourceLoadedStats result = new SzSourceLoadedStats(dataSource);
        try {
            queryingDatabase(timers, "selectCountsForSource");
            try {
                // prepare the statement
                ps = conn.prepareStatement("SELECT entity_count, record_count "
                        + "FROM sz_dm_report WHERE report=? AND statistic=? "
                        + "AND data_source1 = ?");

                // bind the parameters
                ps.setString(1, DATA_SOURCE_SUMMARY.getCode());
                ps.setString(2, ENTITY_COUNT.toString());
                ps.setString(3, dataSource);

                // execute the query
                rs = ps.executeQuery();

                // check if we have a result
                if (rs.next()) {
                    long entityCount = rs.getLong(1);
                    long recordCount = rs.getLong(2);

                    result.setEntityCount(entityCount);
                    result.setRecordCount(recordCount);

                } else {
                    logWarning("Failed to find entity and record count stats " 
                               + "for data source: " + dataSource);
                }

            } finally {
                queriedDatabase(timers, "selectCountsForSource");
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            queryingDatabase(timers, "selectUnmatchedCountForSource");
            try {
                // now get the counts by data source
                ps = conn.prepareStatement(
                        "SELECT entity_count " 
                        + "FROM sz_dm_report WHERE report=? "
                        + "AND statistic=? AND data_source1 = ?");

                // bind the parameters
                ps.setString(1, DATA_SOURCE_SUMMARY.getCode());
                ps.setString(2, UNMATCHED_COUNT.toString());
                ps.setString(3, dataSource);

                // execute the query
                rs = ps.executeQuery();
                if (rs.next()) {
                    long unmatchedCount = rs.getLong(1);

                    // set the unmatched record count
                    result.setUnmatchedRecordCount(unmatchedCount);

                } else {
                    logWarning("Failed to find unmatched record count stats " 
                               + "for data source: " + dataSource);
                }

            } finally {
                queriedDatabase(timers, "selectUnmatchedCountForSource");
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Retrieves a page of entity ID's for entities that have records loaded from
     * the specified data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source for which the entities
     *                      are being retrieved.
     * @param entityIdBound The bounded value for the returned entity ID's,
     *                      formatted as an integer or the word <code>"max"</code>.
     * @param boundType     The {@link SzBoundType} that describes how to apply the
     *                      specified entity ID bound.
     * @param pageSize      The maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers        The optional {@link Timers} to track timing of the
     *                      operation.
     * 
     * @return The {@link SzEntitiesPage} containing the entity IDs.
     * 
     * @throws NullPointerException If a required parameter is specified as
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified page size or sample size
     *                                  is less than one (1), or if the sample size
     *                                  is specified and is greater-than or equal
     *                                  to the sample size.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntitiesPage getEntityIdsForDataSource(Connection   conn,
                                                           String       dataSource,
                                                           String       entityIdBound, 
                                                           SzBoundType  boundType,
                                                           Integer      pageSize,
                                                           Integer      sampleSize,
                                                           Timers       timers) 
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        Objects.requireNonNull(dataSource, "The data source cannot be null");

        SzReportKey reportKey 
            = new SzReportKey(DATA_SOURCE_SUMMARY, ENTITY_COUNT, dataSource, dataSource);

        return retrieveEntitiesPage(
            conn, reportKey, entityIdBound, boundType, pageSize, sampleSize, timers);
    }

}
