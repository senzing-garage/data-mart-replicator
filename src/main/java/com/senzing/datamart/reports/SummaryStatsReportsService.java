package com.senzing.datamart.reports;

import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.ProducesJson;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.Default;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzCrossSourceMatchCounts;
import com.senzing.datamart.reports.model.SzCrossSourceRelationCounts;
import com.senzing.datamart.reports.model.SzCrossSourceSummary;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzRelationsPage;
import com.senzing.datamart.reports.model.SzSourceSummary;
import com.senzing.datamart.reports.model.SzSummaryStats;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.util.LoggingUtilities.formatStackTrace;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * Provides services for the Summary Statistic reports.
 */
public interface SummaryStatsReportsService extends ReportsService {
    /**
     * The prefix path for entity size reports services.
     */
    String SUMMARY_STATS_PREFIX = REPORTS_PREFIX + "/summary";

    /**
     * The endpoint for the summary statistics report.
     */
    String SUMMARY_STATS_ENDPOINT = SUMMARY_STATS_PREFIX + "/";

    /**
     * The endpoint for the source summary report.
     */
    String SOURCE_SUMMARY_ENDPOINT = SUMMARY_STATS_PREFIX + "/data-sources/{dataSourceCode}";

    /**
     * The endpoint for the cross-source summary report.
     */
    String CROSS_SOURCE_SUMMARY_ENDPOINT = SOURCE_SUMMARY_ENDPOINT + "/vs/{vsDataSourceCode}";

    /**
     * The endpoint for the cross-source summary matches report.
     */
    String CROSS_SOURCE_MATCH_SUMMARY_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT + "/matches";

    /**
     * The endpoint for the cross-source summary ambiguous matches report.
     */
    String CROSS_SOURCE_AMBIGUOUS_MATCH_SUMMARY_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT + "/ambiguous-matches";

    /**
     * The endpoint for the cross-source summary possible matches report.
     */
    String CROSS_SOURCE_POSSIBLE_MATCH_SUMMARY_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT + "/possible-matches";

    /**
     * The endpoint for the cross-source summary possible relations report.
     */
    String CROSS_SOURCE_POSSIBLE_RELATION_SUMMARY_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT + "/possible-relations";

    /**
     * The endpoint for the cross-source summary disclosed relations report.
     */
    String CROSS_SOURCE_DISCLOSED_RELATION_SUMMARY_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT + "/disclosed-relations";

    /**
     * The endpoint for the report of entities contributing to the source summary
     * report matches.
     */
    String SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT = SOURCE_SUMMARY_ENDPOINT + "/matches/entities";

    /**
     * The endpoint for the report of entities contributing to the source summary
     * report ambiguous matches.
     */
    String SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT = SOURCE_SUMMARY_ENDPOINT + "/ambiguous-matches/entities";

    /**
     * The endpoint for the report of entities contributing to the source summary
     * report possible matches.
     */
    String SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT = SOURCE_SUMMARY_ENDPOINT + "/possible-matches/entities";

    /**
     * The endpoint for the report of entities contributing to the source summary
     * report possible relations.
     */
    String SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT = SOURCE_SUMMARY_ENDPOINT
            + "/possible-relations/entities";

    /**
     * The endpoint for the report of entities contributing to the source summary
     * report disclosed relations.
     */
    String SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT = SOURCE_SUMMARY_ENDPOINT
            + "/disclosed-relations/entities";

    /**
     * The endpoint for the report of entities contributing to the cross-source
     * summary report matches.
     */
    String CROSS_SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT + "/matches/entities";

    /**
     * The endpoint for the report of entities contributing to the cross-source
     * summary report ambiguous matches.
     */
    String CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/ambiguous-matches/entities";

    /**
     * The endpoint for the report of entities contributing to the cross-source
     * summary report possible matches.
     */
    String CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/possible-matches/entities";

    /**
     * The endpoint for the report of entities contributing to the cross-source
     * summary report possible relations.
     */
    String CROSS_SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/possible-relations/entities";

    /**
     * The endpoint for the report of entities contributing to the cross-source
     * summary report disclosed relations.
     */
    String CROSS_SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/disclosed-relations/entities";

    /**
     * The endpoint for the report of relations contributing to the cross-source
     * summary report ambiguous matches.
     */
    String CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_RELATIONS_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/ambiguous-matches/relations";

    /**
     * The endpoint for the report of relations contributing to the cross-source
     * summary report possible matches.
     */
    String CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_RELATIONS_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/possible-matches/relations";

    /**
     * The endpoint for the report of relations contributing to the cross-source
     * summary report possible relations.
     */
    String CROSS_SOURCE_SUMMARY_POSSIBLE_RELATIONS_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/possible-relations/relations";

    /**
     * The endpoint for the report of relations contributing to the cross-source
     * summary report disclosed relations.
     */
    String CROSS_SOURCE_SUMMARY_DISCLOSED_RELATIONS_ENDPOINT = CROSS_SOURCE_SUMMARY_ENDPOINT
            + "/disclosed-relations/relations";

    /**
     * Exposes
     * {@link SummaryStatsReports#getSummaryStatistics(Connection, String, String, Set, Timers)}
     * as a REST/JSON service at {@link #SUMMARY_STATS_ENDPOINT}.
     * 
     * @param matchKey   The optional match key for retrieving statistics specific
     *                   to a match key, or asterisk (<code>"*"</code>) for all
     *                   match keys, or <code>null</code> for only retrieving
     *                   statistics that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics specific
     *                   to a principle, or asterisk (<code>"*"</code>) for all
     *                   principles, or <code>null</code> for only retrieving
     *                   statistics that are not specific to a principle.
     * @param onlyLoaded Set to <code>true</code> to only consider data sources that
     *                   have loaded record, otherwise set this to
     *                   <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzSummaryStats} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(SUMMARY_STATS_PREFIX)
    @Path(SUMMARY_STATS_ENDPOINT)
    @ProducesJson
    default SzSummaryStats getSummaryStats(
            @Param("matchKey") @Nullable                    String  matchKey, 
            @Param("principle") @Nullable                   String  principle, 
            @Param("onlyLoadedSources") @Default("true")    boolean onlyLoaded) 
        throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            Set<String> dataSources = (onlyLoaded) ? null : this.getConfiguredDataSources(true);

            return SummaryStatsReports.getSummaryStatistics(conn, matchKey, principle, dataSources, timers);

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
     * {@link SummaryStatsReports#getSourceSummary(Connection, String, String, String, Set, Timers)}
     * as a REST/JSON service at {@link #SOURCE_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource The data source code for which the report is being
     *                   requested.
     * @param matchKey   The optional match key for retrieving statistics specific
     *                   to a match key, or asterisk (<code>"*"</code>) for all
     *                   match keys, or <code>null</code> for only retrieving
     *                   statistics that are not specific to a match key.
     * @param principle  The optional principle for retrieving statistics specific
     *                   to a principle, or asterisk (<code>"*"</code>) for all
     *                   principles, or <code>null</code> for only retrieving
     *                   statistics that are not specific to a principle.
     * @param onlyLoaded Set to <code>true</code> to only consider data sources that
     *                   have loaded record, otherwise set this to
     *                   <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzSourceSummary} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(SOURCE_SUMMARY_ENDPOINT)
    @ProducesJson
    default SzSourceSummary getSourceSummary(@Param("dataSourceCode") String dataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            Set<String> dataSources = (onlyLoaded) ? null : this.getConfiguredDataSources(true);

            return SummaryStatsReports.getSourceSummary(conn, dataSource, matchKey, principle, dataSources, timers);

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
     * {@link SummaryStatsReports#getCrossSourceSummary(Connection, String, String, String, String, Timers)}
     * as a REST/JSON service at {@link #CROSS_SOURCE_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource   The data source code identifying the data source for the
     *                     cross-source report being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data source
     *                     for which the cross-source report being requested.
     * 
     * @param matchKey     The optional match key for retrieving statistics specific
     *                     to a match key, or asterisk (<code>"*"</code>) for all
     *                     match keys, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics specific
     *                     to a principle, or asterisk (<code>"*"</code>) for all
     *                     principles, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a principle.
     * @param onlyLoaded   Set to <code>true</code> to only consider data sources
     *                     that have loaded record, otherwise set this to
     *                     <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzCrossSourceSummary} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(CROSS_SOURCE_SUMMARY_ENDPOINT)
    @ProducesJson
    default SzCrossSourceSummary getCrossSourceSummary(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossSourceSummary(conn, dataSource, vsDataSource, matchKey, principle,
                    timers);

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
     * {@link SummaryStatsReports#getCrossSourceMatchSummary( Connection, String, String, String, String, Timers)}
     * as a REST/JSON service at {@link #CROSS_SOURCE_MATCH_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource   The data source code identifying the data source for the
     *                     cross-source report being requested.
     * @param vsDataSource The data source code identifying the "versus" data source
     *                     for which the cross-source report being requested.
     * @param matchKey     The optional match key for retrieving statistics specific
     *                     to a match key, or asterisk (<code>"*"</code>) for all
     *                     match keys, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics specific
     *                     to a principle, or asterisk (<code>"*"</code>) for all
     *                     principles, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a principle.
     * @param onlyLoaded   Set to <code>true</code> to only consider data sources
     *                     that have loaded record, otherwise set this to
     *                     <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzCrossSourceMatchCounts} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_MATCH_SUMMARY_ENDPOINT)
    @Path(CROSS_SOURCE_MATCH_SUMMARY_ENDPOINT + "/")
    @ProducesJson
    default SzCrossSourceMatchCounts getCrossSourceMatchSummary(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossSourceMatchSummary(conn, dataSource, vsDataSource, matchKey, principle,
                    timers);

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
     * {@link SummaryStatsReports#getCrossSourceAmbiguousMatchSummary( Connection, String, String, String, String, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_AMBIGUOUS_MATCH_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource   The data source code identifying the data source for the
     *                     cross-source report being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data source
     *                     for which the cross-source report being requested.
     * @param matchKey     The optional match key for retrieving statistics specific
     *                     to a match key, or asterisk (<code>"*"</code>) for all
     *                     match keys, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics specific
     *                     to a principle, or asterisk (<code>"*"</code>) for all
     *                     principles, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a principle.
     * @param onlyLoaded   Set to <code>true</code> to only consider data sources
     *                     that have loaded record, otherwise set this to
     *                     <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(CROSS_SOURCE_AMBIGUOUS_MATCH_SUMMARY_ENDPOINT)
    @ProducesJson
    default SzCrossSourceRelationCounts getCrossSourceAmbiguousMatchSummary(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossSourceAmbiguousMatchSummary(conn, dataSource, vsDataSource, matchKey,
                    principle, timers);

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
     * {@link SummaryStatsReports#getCrossSourcePossibleMatchSummary( Connection, String, String, String, String, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_POSSIBLE_MATCH_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource   The data source code identifying the data source for the
     *                     cross-source report being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data source
     *                     for which the cross-source report being requested.
     * @param matchKey     The optional match key for retrieving statistics specific
     *                     to a match key, or asterisk (<code>"*"</code>) for all
     *                     match keys, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics specific
     *                     to a principle, or asterisk (<code>"*"</code>) for all
     *                     principles, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a principle.
     * @param onlyLoaded   Set to <code>true</code> to only consider data sources
     *                     that have loaded record, otherwise set this to
     *                     <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_POSSIBLE_MATCH_SUMMARY_ENDPOINT)
    @Path(CROSS_SOURCE_POSSIBLE_MATCH_SUMMARY_ENDPOINT + "/")
    @ProducesJson
    default SzCrossSourceRelationCounts getCrossSourcePossibleMatchSummary(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossSourcePossibleMatchSummary(conn, dataSource, vsDataSource, matchKey,
                    principle, timers);

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
     * {@link SummaryStatsReports#getCrossSourcePossibleRelationSummary( Connection, String, String, String, String, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_POSSIBLE_RELATION_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource   The data source code identifying the data source for the
     *                     cross-source report being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data source
     *                     for which the cross-source report being requested.
     * @param matchKey     The optional match key for retrieving statistics specific
     *                     to a match key, or asterisk (<code>"*"</code>) for all
     *                     match keys, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics specific
     *                     to a principle, or asterisk (<code>"*"</code>) for all
     *                     principles, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a principle.
     * @param onlyLoaded   Set to <code>true</code> to only consider data sources
     *                     that have loaded record, otherwise set this to
     *                     <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_POSSIBLE_RELATION_SUMMARY_ENDPOINT)
    @Path(CROSS_SOURCE_POSSIBLE_RELATION_SUMMARY_ENDPOINT + "/")
    @ProducesJson
    default SzCrossSourceRelationCounts getCrossSourcePossibleRelationSummary(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossSourcePossibleRelationSummary(conn, dataSource, vsDataSource, matchKey,
                    principle, timers);

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
     * {@link SummaryStatsReports#getCrossSourceDisclosedRelationSummary( Connection, String, String, String, String, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_DISCLOSED_RELATION_SUMMARY_ENDPOINT}.
     * 
     * @param dataSource   The data source code identifying the data source for the
     *                     cross-source report being requested.
     * 
     * @param vsDataSource The data source code identifying the "versus" data source
     *                     for which the cross-source report being requested.
     * @param matchKey     The optional match key for retrieving statistics specific
     *                     to a match key, or asterisk (<code>"*"</code>) for all
     *                     match keys, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a match key.
     * @param principle    The optional principle for retrieving statistics specific
     *                     to a principle, or asterisk (<code>"*"</code>) for all
     *                     principles, or <code>null</code> for only retrieving
     *                     statistics that are not specific to a principle.
     * @param onlyLoaded   Set to <code>true</code> to only consider data sources
     *                     that have loaded record, otherwise set this to
     *                     <code>false</code> to consider all data sources.
     * 
     * @return The {@link SzCrossSourceRelationCounts} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_DISCLOSED_RELATION_SUMMARY_ENDPOINT)
    @Path(CROSS_SOURCE_DISCLOSED_RELATION_SUMMARY_ENDPOINT + "/")
    @ProducesJson
    default SzCrossSourceRelationCounts getCrossSourceDisclosedRelationSummary(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("onlyLoadedSources") @Default("true") boolean onlyLoaded) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossSourceDisclosedRelationSummary(conn, dataSource, vsDataSource, matchKey,
                    principle, timers);

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
     * {@link SummaryStatsReports#getSummaryMatchEntityIds(Connection, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at {@link #SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for the
     *                      report being requested.
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT)
    @Path(SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryMatchEntityIds(@Param("dataSourceCode") String dataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryMatchEntityIds(conn, dataSource, matchKey, principle, entityIdBound,
                    boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryAmbiguousMatchEntityIds(Connection, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for the
     *                      report being requested.
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT)
    @Path(SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryAmbiguousMatchEntityIds(@Param("dataSourceCode") String dataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryAmbiguousMatchEntityIds(conn, dataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryPossibleMatchEntityIds(Connection, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for the
     *                      report being requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT)
    @Path(SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryPossibleMatchEntityIds(@Param("dataSourceCode") String dataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryPossibleMatchEntityIds(conn, dataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryPossibleRelationEntityIds(Connection, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for the
     *                      report being requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT)
    @Path(SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryPossibleRelationEntityIds(@Param("dataSourceCode") String dataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryPossibleRelationEntityIds(conn, dataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryDisclosedRelationEntityIds(Connection, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for the
     *                      report being requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT)
    @Path(SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryDisclosedRelatedEntityIds(@Param("dataSourceCode") String dataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryDisclosedRelationEntityIds(conn, dataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getCrossMatchEntityIds(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for the
     *                      cross-source report being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(CROSS_SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_MATCH_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryMatchEntityIds(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossMatchEntityIds(conn, dataSource, vsDataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getCrossAmbiguousMatchEntityIds(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryAmbiguousMatchEntityIds(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossAmbiguousMatchEntityIds(conn, dataSource, vsDataSource, matchKey,
                    principle, entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getCrossPossibleMatchEntityIds(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryPossibleMatchEntityIds(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossPossibleMatchEntityIds(conn, dataSource, vsDataSource, matchKey,
                    principle, entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getCrossPossibleRelationEntityIds(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_RELATION_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryPossibleRelationEntityIds(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossPossibleRelationEntityIds(conn, dataSource, vsDataSource, matchKey,
                    principle, entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getCrossDisclosedRelationEntityIds(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
    @Path(CROSS_SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_DISCLOSED_RELATION_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getSummaryDisclosedRelationEntityIds(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getCrossDisclosedRelationEntityIds(conn, dataSource, vsDataSource, matchKey,
                    principle, entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryAmbiguousMatches(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_RELATIONS_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
     * @return The {@link SzRelationsPage} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_RELATIONS_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_AMBIGUOUS_MATCH_RELATIONS_ENDPOINT + "/")
    @ProducesJson
    default SzRelationsPage getSummaryAmbiguousMatchRelations(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryAmbiguousMatches(conn, dataSource, vsDataSource, matchKey,
                    principle, entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryPossibleMatches(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_RELATIONS_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
     * @return The {@link SzRelationsPage} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_RELATIONS_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_MATCH_RELATIONS_ENDPOINT + "/")
    @ProducesJson
    default SzRelationsPage getSummaryPossibleMatchRelations(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryPossibleMatches(conn, dataSource, vsDataSource, matchKey,
                    principle, entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryPossibleRelations(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_POSSIBLE_RELATIONS_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
     * @return The {@link SzRelationsPage} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_RELATIONS_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_POSSIBLE_RELATIONS_ENDPOINT + "/")
    @ProducesJson
    default SzRelationsPage getSummaryPossibleRelations(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryPossibleRelations(conn, dataSource, vsDataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
     * {@link SummaryStatsReports#getSummaryDisclosedRelations(Connection, String, String, String, String, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at
     * {@link #CROSS_SOURCE_SUMMARY_DISCLOSED_RELATIONS_ENDPOINT}.
     * 
     * @param dataSource    The data source code identifying the data source for
     *                      which the statistics are being requested.
     * 
     * @param vsDataSource  The data source code identifying the "versus" data
     *                      source for which the cross-source report being
     *                      requested.
     * 
     * @param matchKey      The optional match key for retrieving statistics
     *                      specific to a match key, or asterisk (<code>"*"</code>)
     *                      for all match keys, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a match
     *                      key.
     * @param principle     The optional principle for retrieving statistics
     *                      specific to a principle, or asterisk (<code>"*"</code>)
     *                      for all principles, or <code>null</code> for only
     *                      retrieving statistics that are not specific to a
     *                      principle.
     * 
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
     * @return The {@link SzRelationsPage} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(CROSS_SOURCE_SUMMARY_DISCLOSED_RELATIONS_ENDPOINT)
    @Path(CROSS_SOURCE_SUMMARY_DISCLOSED_RELATIONS_ENDPOINT + "/")
    @ProducesJson
    default SzRelationsPage getSummaryDisclosedRelations(@Param("dataSourceCode") String dataSource, @Param("vsDataSourceCode") String vsDataSource, @Param("matchKey") @Nullable String matchKey, @Param("principle") @Nullable String principle, @Param("bound") @Nullable String entityIdBound, @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType, @Param("pageSize") @Nullable Integer pageSize, @Param("sampleSize") @Nullable Integer sampleSize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return SummaryStatsReports.getSummaryDisclosedRelations(conn, dataSource, vsDataSource, matchKey, principle,
                    entityIdBound, boundType, pageSize, sampleSize, timers);
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
