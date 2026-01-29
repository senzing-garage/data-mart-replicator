package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.senzing.datamart.model.SzReportCode;
import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.model.SzReportStatistic;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzCrossSourceMatchCounts;
import com.senzing.datamart.reports.model.SzCrossSourceRelationCounts;
import com.senzing.datamart.reports.model.SzCrossSourceSummary;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzMatchCounts;
import com.senzing.datamart.reports.model.SzRelationCounts;
import com.senzing.datamart.reports.model.SzRelationsPage;
import com.senzing.datamart.reports.model.SzSourceSummary;
import com.senzing.datamart.reports.model.SzSummaryStats;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.reports.ReportUtilities.*;
import static com.senzing.datamart.reports.model.SzRelationType.*;
import static com.senzing.datamart.model.SzReportStatistic.*;

/**
 * Provides Summary Stats Report functionality.
 */
public final class SummaryStatsReports {
    /**
     * Thread-local flag used to optimize handling of data sources set.
     */
    private static final ThreadLocal<Boolean> AUGMENT_DATA_SOURCES 
        = new ThreadLocal<>() {
            @Override
            protected Boolean initialValue() {
                return Boolean.TRUE;
            }
        };

    /**
     * Private default constructor.
     */
    private SummaryStatsReports() {
        // do nothing
    }

    /**
     * Gets the {@link SzSummaryStats} describing the source summaries for
     * one or more data sources.
     * 
     * <p>
     * The statistics for data sources with loaded records are <b>ALWAYS</b> included
     * in the report.  However, a {@link Set} of {@link String} data source codes can
     * be specified for those data sources for which "zero" statistics should be
     * included even if no records are loaded for those data sources.  <b>NOTE:</b>
     * this will <b>NOT</b> filter statistics so that statistics for data sources
     * with loaded records can be excluded.
     * 
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * 
     * @param matchKey The optional match key for retrieving statistics specific
     *                 to a match key, or asterisk (<code>"*"</code>) for all
     *                 match keys, or <code>null</code> for only retrieving
     *                 statistics that are not specific to a match key.
     * 
     * @param principle  The optional principle for retrieving statistics specific
     *                   to a principle, or asterisk (<code>"*"</code>) for all
     *                   principles, or <code>null</code> for only retrieving
     *                   statistics that are not specific to a principle.
     * 
     * @param dataSources The optional {@link Set} of {@link String} data source codes
     *                    for those data sources to be included in the statistics even
     *                    if they have no records loaded, or <code>null</code> if the
     *                    results should simply include statistics for data sources for
     *                    which records have been loaded.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzSummaryStats} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzSummaryStats getSummaryStatistics(Connection    conn,
                                                      String        matchKey,
                                                      String        principle, 
                                                      Set<String>   dataSources,
                                                      Timers        timers) 
        throws NullPointerException, SQLException
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        
        // create the result
        SzSummaryStats result = new SzSummaryStats();

        // get the data sources to report on
        Set<String> reportSources = getLoadedDataSources(conn, timers);
        if (dataSources != null) {
            reportSources.addAll(dataSources);
        }

        // iterate over the data sources
        Boolean initialValue = AUGMENT_DATA_SOURCES.get();
        AUGMENT_DATA_SOURCES.set(Boolean.FALSE);
        try {
            for (String dataSource : reportSources) {
                result.addSourceSummary(getSourceSummary(
                    conn, dataSource, matchKey, principle, reportSources, timers));
            }
        } finally {
            AUGMENT_DATA_SOURCES.set(initialValue);
        }

        // return the result
        return result;
    }

    /**
     * Gets the {@link SzSourceSummary} (including cross-summary statistics) 
     * for a specific data source.
     * 
     * <p>
     * The cross-summary statistics for data sources with loaded records are
     * <b>ALWAYS</b> included in the report.  However, a {@link Set} of 
     * {@link String} data source codes can be specified for those data sources
     * for which "zero" statistics should be included even if no records are
     * loaded for those data sources.  <b>NOTE:</b> this will <b>NOT</b> filter
     * statistics so that cross-summary statistics for data sources with loaded
     * records can be excluded.
     *
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param dataSource The data source code identifying the data source for
     *                   which the count statistics are being requested.
     * 
     * @param matchKey The optional match key for retrieving statistics specific 
     *                 to a match key, or asterisk (<code>"*"</code>) for all
     *                 match keys, or <code>null</code> for only retrieving
     *                 statistics that are not specific to a match key.
     * 
     * @param principle The optional principle for retrieving statistics specific
     *                  to a principle, or asterisk (<code>"*"</code>) for all
     *                  principles, or <code>null</code> for only retrieving
     *                  statistics that are not specific to a principle.
     * 
     * @param dataSources The optional {@link Set} of {@link String} data source codes
     *                    for those data sources to be included in the statistics even
     *                    if they have no records loaded, or <code>null</code> if the
     *                    results should simply include statistics for data sources for
     *                    which records have been loaded.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzSourceSummary} describing the statistics.
     * 
     * @throws NullPointerException If a required parameter is specified as 
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzSourceSummary getSourceSummary(Connection   conn,
                                                   String       dataSource,
                                                   String       matchKey, 
                                                   String       principle, 
                                                   Set<String>  dataSources,
                                                   Timers       timers)
        throws NullPointerException, SQLException
    {
        // get the connection
        PreparedStatement ps = null;
        ResultSet rs = null;
        SzSourceSummary result = new SzSourceSummary(dataSource);
        try {
            // get the connection to the data mart database
            queryingDatabase(timers, "selectSourceSummary");
            try {
                // prepare the statement
                ps = conn.prepareStatement(
                    "SELECT statistic, entity_count, record_count "
                    + "FROM sz_dm_report WHERE report=? " 
                    + "AND data_source1 = ? AND data_source2 = ? "
                    + "AND statistic IN (?, ?)" 
                    + "ORDER BY statistic");

                // bind the parameters
                ps.setString(1, DATA_SOURCE_SUMMARY.getCode());
                ps.setString(2, dataSource);
                ps.setString(3, dataSource);
                ps.setString(4, ENTITY_COUNT.toString());
                ps.setString(5, UNMATCHED_COUNT.toString());

                // execute the query
                rs = ps.executeQuery();

                // check if we have a result
                while (rs.next()) {
                    String encodedStat = rs.getString(1);
                    long entityCount = rs.getLong(2);
                    long recordCount = rs.getLong(3);

                    SzReportStatistic.Formatter formatter
                        = SzReportStatistic.Formatter.parse(encodedStat);

                    SzReportStatistic statistic = formatter.getStatistic();
                    switch (statistic) {
                    case ENTITY_COUNT:
                        result.setEntityCount(entityCount);
                        result.setRecordCount(recordCount);
                        break;
                    case UNMATCHED_COUNT:
                        result.setUnmatchedRecordCount(recordCount);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected statistic value: " + statistic);
                    }
                }

            } finally {
                queriedDatabase(timers, "selectSourceSummary");
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // check if we should only consider loaded data sources
            Set<String> reportSources = dataSources;
            if (dataSources == null || AUGMENT_DATA_SOURCES.get()) {
                reportSources = getLoadedDataSources(conn, timers);
                if (dataSources != null) {
                    reportSources.addAll(dataSources);
                }
            }

            // get the cross summaries
            for (String vsDataSource : reportSources) {
                result.addCrossSourceSummary(getCrossSourceSummary(
                    conn, dataSource, vsDataSource, null, matchKey, principle, timers));
            }

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets the data sources that have loaded records.
     * 
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link Set} of data sources that have loaded records.
     * 
     * @throws NullPointerException If the specified JDBC {@link Connection}
     *                              is <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    private static SortedSet<String> getLoadedDataSources(Connection    conn, 
                                                          Timers        timers) 
        throws NullPointerException, SQLException 
    {
        // initialize resources
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            // prepare the statement
            queryingDatabase(timers, "selectLoadedSources");
            ps = conn.prepareStatement("SELECT data_source1 FROM sz_dm_report "
                    + "WHERE report=? AND statistic=? AND record_count > 0 " 
                    + "ORDER BY data_source1");

            // bind the parameters
            ps.setString(1, DATA_SOURCE_SUMMARY.getCode());
            ps.setString(2, ENTITY_COUNT.toString());

            // execute the query
            rs = ps.executeQuery();

            // read the results
            SortedSet<String> dataSources = new TreeSet<>();
            while (rs.next()) {
                dataSources.add(rs.getString(1));
            }

            // return the data sources
            return dataSources;

        } finally {
            queriedDatabase(timers, "selectLoadedSources");

            // release resources
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets cross-source summary statistics for a specific primary data source and
     * "versus" data source.
     *
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param dataSource   The data source code identifying the primary data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param matchKey         The optional match key for retrieving statistics
     *                         specific to a match key, or asterisk
     *                         (<code>"*"</code>) for all match keys, or
     *                         <code>null</code> for only retrieving statistics that
     *                         are not specific to a match key.
     * 
     * @param principle        The optional principle for retrieving statistics
     *                         specific to a principle, or asterisk
     *                         (<code>"*"</code>) for all principles, or
     *                         <code>null</code> for only retrieving statistics that
     *                         are not specific to a principle.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzCrossSourceSummary} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzCrossSourceSummary getCrossSourceSummary(Connection conn,
                                                             String     dataSource, 
                                                             String     vsDataSource, 
                                                             String     matchKey,
                                                             String     principle, 
                                                             Timers     timers) 
        throws NullPointerException, SQLException                                                         
    {
        return getCrossSourceSummary(conn, 
                                     dataSource, 
                                     vsDataSource,
                                     null,
                                     matchKey,
                                     principle,
                                     timers);
    }

    /**
     * Internal method for obtaining the count statistics for a specific data
     * source.
     * 
     * @param conn               The non-null JDBC {@link Connection} to use.
     * @param dataSource         The primary data source code for which the
     *                           statistics are being requested.
     * @param vsDataSource       The "versus" data source code for which the
     *                           statistics are being requested.
     * @param requestedStatistic The optional {@link SzReportStatistic} to narrow
     *                           the query, or <code>null</code> if all statistics 
     *                           should be included.
     * @param requestedMatchKey  The optional match key for retrieving statistics
     *                           specific to a match key, or asterisk
     *                           (<code>"*"</code>) for all match keys, or
     *                           <code>null</code> for only retrieving statistics
     *                           that are not specific to a match key.
     * @param requestedPrinciple The optional principle for retrieving statistics
     *                           specific to a principle, or asterisk
     *                           (<code>"*"</code>) for all principles, or
     *                           <code>null</code> for only retrieving statistics
     *                           that are not specific to a principle.
     * @param timers             The {@link Timers} associated with the request.
     * 
     * @return The {@link SzCrossSourceSummary} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    protected static SzCrossSourceSummary getCrossSourceSummary(
            Connection          conn,
            String              dataSource, 
            String              vsDataSource, 
            SzReportStatistic   requestedStatistic, 
            String              requestedMatchKey, 
            String              requestedPrinciple, 
            Timers              timers) 
        throws NullPointerException, SQLException
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        
        // normalize the match key and principle
        if (requestedMatchKey != null) {
            requestedMatchKey = requestedMatchKey.trim();
            if (requestedMatchKey.length() == 0) {
                requestedMatchKey = null;
            }
        }
        if (requestedPrinciple != null) {
            requestedPrinciple = requestedPrinciple.trim();
            if (requestedPrinciple.length() == 0) {
                requestedPrinciple = null;
            }
        }

        // keep counts
        int matchCount = 0;
        int ambMatchCount = 0;
        int posMatchCount = 0;
        int posRelCount = 0;
        int discRelCount = 0;

        // get the connection
        PreparedStatement ps = null;
        ResultSet rs = null;
        SzCrossSourceSummary result = new SzCrossSourceSummary(dataSource, vsDataSource);

        try {
            queryingDatabase(timers, "selectCrossSourceSummary");
            try {
                // determine the report code
                String reportCode = (dataSource.equals(vsDataSource)) 
                    ? DATA_SOURCE_SUMMARY.getCode() : CROSS_SOURCE_SUMMARY.getCode();

                // prepare the statement
                ps = conn.prepareStatement(
                    "SELECT statistic, entity_count, record_count, relation_count "
                    + "FROM sz_dm_report WHERE report=? AND data_source1 = ? "
                    + "AND data_source2 = ? AND statistic NOT IN (?, ?) "
                    + ((requestedStatistic != null) ? "AND statistic LIKE ? " : "") 
                    + "ORDER BY statistic");

                // bind the parameters
                ps.setString(1, reportCode);
                ps.setString(2, dataSource);
                ps.setString(3, vsDataSource);
                ps.setString(4, ENTITY_COUNT.toString());
                ps.setString(5, UNMATCHED_COUNT.toString());
                if (requestedStatistic != null) {
                    ps.setString(6, requestedStatistic.toString() + "%");
                }

                // execute the query
                rs = ps.executeQuery();

                // check if we have a result
                while (rs.next()) {
                    String encodedStat = rs.getString(1);
                    long entityCount = rs.getLong(2);
                    long recordCount = rs.getLong(3);
                    long relationCount = rs.getLong(4);

                    SzReportStatistic.Formatter formatter
                        = SzReportStatistic.Formatter.parse(encodedStat);

                    SzReportStatistic statistic = formatter.getStatistic();
                    String principle = formatter.getPrinciple();
                    String matchKey = formatter.getMatchKey();

                    // check the statistic
                    if (requestedStatistic != null 
                        && requestedStatistic != statistic) 
                    {
                        continue;
                    }

                    // filter on match key and principle
                    if (!Objects.equals(principle, requestedPrinciple) 
                        && !"*".equals(requestedPrinciple)) 
                    {
                        continue;
                    }
                    if (!Objects.equals(matchKey, requestedMatchKey) 
                        && !"*".equals(requestedMatchKey)) 
                    {
                        continue;
                    }

                    SzMatchCounts matchCounts = null;
                    SzRelationCounts relationCounts = null;

                    switch (statistic) {
                        case MATCHED_COUNT:
                            matchCounts = new SzMatchCounts(matchKey, principle);
                            matchCounts.setEntityCount(entityCount);
                            matchCounts.setRecordCount(recordCount);
                            break;
                        case AMBIGUOUS_MATCH_COUNT:
                        case POSSIBLE_MATCH_COUNT:
                        case POSSIBLE_RELATION_COUNT:
                        case DISCLOSED_RELATION_COUNT:
                            relationCounts = new SzRelationCounts(matchKey, principle);
                            relationCounts.setEntityCount(entityCount);
                            relationCounts.setRecordCount(recordCount);
                            relationCounts.setRelationCount(relationCount);
                            break;
                        default:
                            throw new IllegalStateException(
                                "Unexpected statistic encountered.  statistic=[ "
                                + statistic + " ], formattedStatistic=[ " 
                                + encodedStat + " ]");
                    }
                    switch (statistic) {
                        case MATCHED_COUNT:
                            matchCount++;
                            result.addMatches(matchCounts);
                            break;
                        case AMBIGUOUS_MATCH_COUNT:
                            ambMatchCount++;
                            result.addAmbiguousMatches(relationCounts);
                            break;
                        case POSSIBLE_MATCH_COUNT:
                            posMatchCount++;
                            result.addPossibleMatches(relationCounts);
                            break;
                        case POSSIBLE_RELATION_COUNT:
                            posRelCount++;
                            result.addPossibleRelations(relationCounts);
                            break;
                        case DISCLOSED_RELATION_COUNT:
                            discRelCount++;
                            result.addDisclosedRelations(relationCounts);
                            break;
                        default:
                            throw new IllegalStateException(
                                "Unexpected statistic encountered.  statistic=[ "
                                + statistic + " ], formattedStatistic=[ " 
                                + encodedStat + " ]");
                    }
                }

            } finally {
                queriedDatabase(timers, "selectCrossSourceSummary");
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // handle the zeroes
            SzReportStatistic stat = requestedStatistic;
            String mkey = requestedMatchKey;
            String prin = requestedPrinciple;
            if ("*".equals(mkey)) {
                mkey = null;
            }
            if ("*".equals(prin)) {
                prin = null;
            }
            if (matchCount == 0 && (stat == null || stat == MATCHED_COUNT)) {
                result.addMatches(new SzMatchCounts(mkey, prin));
            }
            if (ambMatchCount == 0 && (stat == null || stat == AMBIGUOUS_MATCH_COUNT)) {
                result.addAmbiguousMatches(new SzRelationCounts(mkey, prin));
            }
            if (posMatchCount == 0 && (stat == null || stat == POSSIBLE_MATCH_COUNT)) {
                result.addPossibleMatches(new SzRelationCounts(mkey, prin));
            }
            if (posRelCount == 0 && (stat == null || stat == POSSIBLE_RELATION_COUNT)) {
                result.addPossibleRelations(new SzRelationCounts(mkey, prin));
            }
            if (discRelCount == 0 && (stat == null || stat == DISCLOSED_RELATION_COUNT)) {
                result.addDisclosedRelations(new SzRelationCounts(mkey, prin));
            }

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets the cross-summary statistics for matches for entities having at least
     * one record from a primary data source and at least one <b>other</b> record
     * from another data source (which may be the same data source), optionally for
     * one or more combination of match key and principle.
     *
     * @param conn         The non-null JDBC {@link Connection} to use.
     * @param dataSource   The data source code identifying the primary data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * @param matchKey     The optional match key for retrieving statistics
     *                     specific to a match key, or asterisk
     *                     (<code>"*"</code>) for all match keys, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics
     *                     specific to a principle, or asterisk
     *                     (<code>"*"</code>) for all principles, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a principle.
     * @param timers       The optional {@link Timers} to track timing of the 
     *                     operation.
     * 
     * @return The {@link SzCrossSourceMatchCounts} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzCrossSourceMatchCounts getCrossSourceMatchSummary(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey, 
            String      principle,
            Timers      timers)
        throws NullPointerException, SQLException
    {
        SzCrossSourceSummary summary = getCrossSourceSummary(
            conn, dataSource, vsDataSource, MATCHED_COUNT, matchKey, principle, timers);
            
        SzCrossSourceMatchCounts result 
            = new SzCrossSourceMatchCounts(dataSource, vsDataSource);

        result.setCounts(summary.getMatches());

        return result;
    }

    /**
     * Gets the cross-summary statistics for ambiguous-match relations between
     * entities having at least one record from one data source and entities having
     * at least one record from another data source (which may be the same),
     * optionally for one or more combination of match key and principle.
     *
     * @param conn         The non-null JDBC {@link Connection} to use.
     * @param dataSource   The data source code identifying the primary data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * @param matchKey     The optional match key for retrieving statistics
     *                     specific to a match key, or asterisk
     *                     (<code>"*"</code>) for all match keys, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics
     *                     specific to a principle, or asterisk
     *                     (<code>"*"</code>) for all principles, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a principle.
     * @param timers       The optional {@link Timers} to track timing of the 
     *                     operation.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzCrossSourceRelationCounts getCrossSourceAmbiguousMatchSummary(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            Timers      timers)
        throws NullPointerException, SQLException
    {
        SzCrossSourceSummary summary = getCrossSourceSummary(conn,
                                                             dataSource,
                                                             vsDataSource,
                                                             AMBIGUOUS_MATCH_COUNT,
                                                             matchKey,
                                                             principle,
                                                             timers);

        SzCrossSourceRelationCounts result 
            = new SzCrossSourceRelationCounts(dataSource, vsDataSource, AMBIGUOUS_MATCH);

        result.setCounts(summary.getAmbiguousMatches());

        return result;
    }

    /**
     * Gets the cross-summary statistics for possible-match relations between
     * entities having at least one record from one data source and entities having
     * at least one record from another data source (which may be the same),
     * optionally for one or more combination of match key and principle.
     *
     * @param conn         The non-null JDBC {@link Connection} to use.
     * @param dataSource   The data source code identifying the primary data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * @param matchKey     The optional match key for retrieving statistics
     *                     specific to a match key, or asterisk
     *                     (<code>"*"</code>) for all match keys, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics
     *                     specific to a principle, or asterisk
     *                     (<code>"*"</code>) for all principles, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a principle.
     * @param timers       The optional {@link Timers} to track timing of the 
     *                     operation.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzCrossSourceRelationCounts getCrossSourcePossibleMatchSummary(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            Timers      timers)
        throws NullPointerException, SQLException
    {
        SzCrossSourceSummary summary = getCrossSourceSummary(conn,
                                                             dataSource,
                                                             vsDataSource,
                                                             POSSIBLE_MATCH_COUNT,
                                                             matchKey,
                                                             principle,
                                                             timers);

        SzCrossSourceRelationCounts result 
            = new SzCrossSourceRelationCounts(dataSource, vsDataSource, POSSIBLE_MATCH);

        result.setCounts(summary.getPossibleMatches());

        return result;
    }

    /**
     * Gets the cross-summary statistics for possible relations between entities
     * having at least one record from one data source and entities having at least
     * one record from another data source (which may be the same), optionally for
     * one or more combination of match key and principle.
     *
     * @param conn         The non-null JDBC {@link Connection} to use.
     * @param dataSource   The data source code identifying the primary data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * @param matchKey     The optional match key for retrieving statistics
     *                     specific to a match key, or asterisk
     *                     (<code>"*"</code>) for all match keys, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics
     *                     specific to a principle, or asterisk
     *                     (<code>"*"</code>) for all principles, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a principle.
     * @param timers       The optional {@link Timers} to track timing of the 
     *                     operation.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzCrossSourceRelationCounts getCrossSourcePossibleRelationSummary(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            Timers      timers)
        throws NullPointerException, SQLException
    {
        SzCrossSourceSummary summary = getCrossSourceSummary(conn,
                                                             dataSource,
                                                             vsDataSource,
                                                             POSSIBLE_RELATION_COUNT,
                                                             matchKey,
                                                             principle,
                                                             timers);

        SzCrossSourceRelationCounts result = new SzCrossSourceRelationCounts(
            dataSource, vsDataSource, POSSIBLE_RELATION);

        result.setCounts(summary.getPossibleRelations());

        return result;
    }

    /**
     * Gets the cross-summary statistics for disclosed relations between entities
     * having at least one record from one data source and entities having at least
     * one record from another data source (which may be the same), optionally for
     * one or more combination of match key and principle.
     *
     * @param conn         The non-null JDBC {@link Connection} to use.
     * @param dataSource   The data source code identifying the primary data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data
     *                     source for which the cross summary statistics are
     *                     being requested.
     * @param matchKey     The optional match key for retrieving statistics
     *                     specific to a match key, or asterisk
     *                     (<code>"*"</code>) for all match keys, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics
     *                     specific to a principle, or asterisk
     *                     (<code>"*"</code>) for all principles, or
     *                     <code>null</code> for only retrieving statistics that
     *                     are not specific to a principle.
     * @param timers       The optional {@link Timers} to track timing of the 
     *                     operation.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzCrossSourceRelationCounts getCrossSourceDisclosedRelationSummary(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            Timers      timers)
        throws NullPointerException, SQLException
    {
        SzCrossSourceSummary summary = getCrossSourceSummary(conn,
                                                             dataSource,
                                                             vsDataSource,
                                                             DISCLOSED_RELATION_COUNT,
                                                             matchKey,
                                                             principle,
                                                             timers);

        SzCrossSourceRelationCounts result = new SzCrossSourceRelationCounts(
            dataSource, vsDataSource, DISCLOSED_RELATION);

        result.setCounts(summary.getDisclosedRelations());

        return result;
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least two records
     * from the associated data source that have matched.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data source
     *                      for which the entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) for all match keys, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) for all principles, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a principle.
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
    public static SzEntitiesPage getSummaryMatchEntityIds(
            Connection  conn,
            String      dataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            dataSource, 
                            MATCHED_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the associated data source ambiguously matched against another entity
     * that has at least one record from the associated data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data source
     *                      for which the entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) for all match keys, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) for all principles, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a principle.
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
    public static SzEntitiesPage getSummaryAmbiguousMatchEntityIds(
            Connection  conn,
            String      dataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            dataSource, 
                            AMBIGUOUS_MATCH_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the associated data source with a possible-match relationship to another
     * entity that has at least one record from the associated data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data source
     *                      for which the entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) for all match keys, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) for all principles, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a principle.
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
    public static SzEntitiesPage getSummaryPossibleMatchEntityIds(
            Connection  conn,
            String      dataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            dataSource, 
                            POSSIBLE_MATCH_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the associated data source with a possible relation to another entity
     * that has at least one record from the associated data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data source
     *                      for which the entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) for all match keys, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) for all principles, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a principle.
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
    public static SzEntitiesPage getSummaryPossibleRelationEntityIds(
            Connection  conn,
            String      dataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            dataSource, 
                            POSSIBLE_RELATION_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the associated data source with a disclosed relationship to another
     * entity that has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data source
     *                      for which the entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) for all match keys, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) for all principles, or
     *                   <code>null</code> for only retrieving statistics
     *                   that are not specific to a principle.
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
    public static SzEntitiesPage getSummaryDisclosedRelationEntityIds(
            Connection  conn,
            String      dataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            dataSource, 
                            DISCLOSED_RELATION_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the first data source and another record from the second "versus" data
     * source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
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
    public static SzEntitiesPage getCrossMatchEntityIds(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            vsDataSource, 
                            MATCHED_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the first data source ambiguously matched against another entity that
     * has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
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
    public static SzEntitiesPage getCrossAmbiguousMatchEntityIds(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            vsDataSource, 
                            AMBIGUOUS_MATCH_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the first data source possibly matched against another entity that has
     * at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
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
    public static SzEntitiesPage getCrossPossibleMatchEntityIds(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            vsDataSource, 
                            POSSIBLE_MATCH_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the associated data source with a disclosed relationship to another
     * entity that has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
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
    public static SzEntitiesPage getCrossPossibleRelationEntityIds(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            vsDataSource, 
                            POSSIBLE_RELATION_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have at least one record
     * from the associated data source ambiguously matched against another entity
     * that has at least one record from the associated data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
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
    public static SzEntitiesPage getCrossDisclosedRelationEntityIds(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      entityIdBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getEntityIds(conn, 
                            dataSource, 
                            vsDataSource, 
                            DISCLOSED_RELATION_COUNT,
                            matchKey,
                            principle,
                            entityIdBound,
                            boundType,
                            pageSize,
                            sampleSize,
                            timers);
    }

    /**
     * Retrieves a page of entity ID's for entities that have the match type
     * associated with the specific {@link SzReportStatistic} using the other
     * parameters for determining the page.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The data source for which the entities are being
     *                      retrieved.
     * @param vsDataSource  The "versus" data source for which the entities are
     *                      being retrieved.
     * @param statistic     The non-null {@link SzReportStatistic} to use.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
     * @param entityIdBound The bound value for the entity ID's that will be
     *                      returned.
     * @param boundType     The {@link SzBoundType} that describes how to apply the
     *                      specified entity ID bound.
     * @param pageSize      The maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers        The optional {@link Timers} to track timing of the
     *                      operation.
     * 
     * @return The {@link SzEntitiesPage} describing the page of entities.
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
    protected static SzEntitiesPage getEntityIds(Connection         conn,
                                                 String             dataSource,
                                                 String             vsDataSource,
                                                 SzReportStatistic  statistic,
                                                 String             matchKey,
                                                 String             principle,
                                                 String             entityIdBound,
                                                 SzBoundType        boundType,
                                                 Integer            pageSize,
                                                 Integer            sampleSize,
                                                 Timers             timers)
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        Objects.requireNonNull(dataSource, "The data source cannot be null");
        Objects.requireNonNull(vsDataSource,
        "The versus data source cannot be null");

        // normalize the match key and principle
        principle = "*".equals(principle) ? null : principle;
        matchKey = "*".equals(matchKey) ? null : matchKey;

        String stat = statistic.principle(principle).matchKey(matchKey).format();

        SzReportCode reportCode = (dataSource.equals(vsDataSource)) 
            ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

        SzReportKey reportKey = new SzReportKey(
            reportCode, stat, dataSource, vsDataSource);

        return retrieveEntitiesPage(conn,
                                    reportKey,
                                    entityIdBound,
                                    boundType,
                                    pageSize,
                                    sampleSize,
                                    timers);
    }

    /**
     * Retrieves a page of {@link com.senzing.datamart.reports.model.SzReportRelation}
     * instances describing the ambiguous match relations between entities having at
     * least one record from the first data source ambiguously matched against another
     * entity that has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
     * @param relationBound The bounded value for the returned relations.
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
    public static SzRelationsPage getSummaryAmbiguousMatchRelations(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      relationBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getRelations(conn, 
                            dataSource,
                            vsDataSource,
                            AMBIGUOUS_MATCH_COUNT,
                            matchKey,
                            principle,
                            relationBound,
                            boundType,
                            pageSize,
                            sampleSize, 
                            timers);
    }

    /**
     * Retrieves a page of {@link com.senzing.datamart.reports.model.SzReportRelation}
     * instances describing the possible match relations between entities having at
     * least one record from the first data source possibly matched against another
     * entity that has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
     * @param relationBound The bounded value for the returned relations.
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
    public static SzRelationsPage getSummaryPossibleMatchRelations(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      relationBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getRelations(conn, 
                            dataSource,
                            vsDataSource,
                            POSSIBLE_MATCH_COUNT,
                            matchKey,
                            principle,
                            relationBound,
                            boundType,
                            pageSize,
                            sampleSize, 
                            timers);
    }

    /**
     * Retrieves a page of {@link com.senzing.datamart.reports.model.SzReportRelation}
     * instances describing the possible relations between entities having at least
     * one record from the first data source possibly related against another entity
     * that has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
     * @param relationBound The bounded value for the returned relations.
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
    public static SzRelationsPage getSummaryPossibleRelations(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      relationBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getRelations(conn, 
                            dataSource,
                            vsDataSource,
                            POSSIBLE_RELATION_COUNT,
                            matchKey,
                            principle,
                            relationBound,
                            boundType,
                            pageSize,
                            sampleSize, 
                            timers);
    }

    /**
     * Retrieves a page of {@link com.senzing.datamart.reports.model.SzReportRelation}
     * instances describing the disclosed relations between entities having at least
     * one record from the first data source having a disclosed relation to another
     * entity that has at least one record from the "versus" data source.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source code identifying the data
     *                      source for which the entities are being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the 
     *                      entities are being retrieved.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
     * @param relationBound The bounded value for the returned relations.
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
    public static SzRelationsPage getSummaryDisclosedRelations(
            Connection  conn,
            String      dataSource,
            String      vsDataSource,
            String      matchKey,
            String      principle,
            String      relationBound,
            SzBoundType boundType,
            Integer     pageSize,
            Integer     sampleSize,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        return getRelations(conn, 
                            dataSource,
                            vsDataSource,
                            DISCLOSED_RELATION_COUNT,
                            matchKey,
                            principle,
                            relationBound,
                            boundType,
                            pageSize,
                            sampleSize, 
                            timers);
    }

    /**
     * Retrieves a page of relations that have the match type associated with the
     * specific {@link SzReportStatistic} using the other parameters for determining
     * the page.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param dataSource    The non-null data source for which the entities are
     *                      being retrieved.
     * @param vsDataSource  The non-null "versus" data source for which the
     *                      entities are being retrieved.
     * @param statistic     The non-null {@link SzReportStatistic} to use.
     * @param matchKey   The optional match key for retrieving statistics
     *                   specific to a match key, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all
     *                   match keys.
     * @param principle  The optional principle for retrieving statistics
     *                   specific to a principle, or asterisk
     *                   (<code>"*"</code>) or <code>null</code> for all 
     *                   principles.
     * @param relationBound The bound value for the relation that is either a single
     *                      entity ID or a pair of entity ID's separated by a colon.
     * @param boundType     The {@link SzBoundType} that describes how to apply the
     *                      specified entity ID bound.
     * @param pageSize      The maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers        The optional {@link Timers} to use.
     * 
     * @return The {@link SzRelationsPage} describing the relations on the page.
     * 
     * @throws NullPointerException If a required parameter is specified as 
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified page size or sample
     *                                  size is less than one (1), or if the
     *                                  sample size is specified and is
     *                                  greater-than or equal to the sample size.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    protected static SzRelationsPage getRelations(
            Connection          conn,
            String              dataSource, 
            String              vsDataSource,
            SzReportStatistic   statistic,
            String              matchKey,
            String              principle,
            String              relationBound, 
            SzBoundType         boundType,
            Integer             pageSize, 
            Integer             sampleSize, 
            Timers              timers) 
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        Objects.requireNonNull(dataSource, "The data source cannot be null");
        Objects.requireNonNull(vsDataSource,
        "The versus data source cannot be null");

        // normalize the match key and principle
        principle = "*".equals(principle) ? null : principle;
        matchKey = "*".equals(matchKey) ? null : matchKey;

        String stat = statistic.principle(principle).matchKey(matchKey).format();

        SzReportCode reportCode = (dataSource.equals(vsDataSource)) 
            ? DATA_SOURCE_SUMMARY : CROSS_SOURCE_SUMMARY;

        SzReportKey reportKey = new SzReportKey(
            reportCode, stat, dataSource, vsDataSource);

        return retrieveRelationsPage(conn, 
                                     reportKey,
                                     relationBound,
                                     boundType,
                                     pageSize,
                                     sampleSize,
                                     timers);

    }    
}
