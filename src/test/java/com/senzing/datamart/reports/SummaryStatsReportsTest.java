package com.senzing.datamart.reports;

import static com.senzing.datamart.model.SzMatchType.AMBIGUOUS_MATCH;
import static com.senzing.datamart.model.SzMatchType.DISCLOSED_RELATION;
import static com.senzing.datamart.model.SzMatchType.POSSIBLE_MATCH;
import static com.senzing.datamart.model.SzMatchType.POSSIBLE_RELATION;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.checkerframework.checker.units.qual.m;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.handlers.CrossSourceKey;
import com.senzing.datamart.handlers.MatchPairKey;
import com.senzing.datamart.model.SzMatchType;
import com.senzing.datamart.model.SzRecord;
import com.senzing.datamart.model.SzRelatedEntity;
import com.senzing.datamart.model.SzRelationship;
import com.senzing.datamart.model.SzResolvedEntity;
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
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.SQLUtilities;

import static com.senzing.datamart.reports.DataSourceCombination.*;
import static com.senzing.datamart.reports.model.SzBoundType.EXCLUSIVE_LOWER;
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class SummaryStatsReportsTest extends AbstractReportsTest {

    private Map<RepositoryType, SzSummaryStats> summaryStatsMap = null;

    @Override
    @BeforeAll
    public void setup() throws Exception {
        super.setup();
        this.summaryStatsMap = this.getSummaryStats();
    }

    protected Set<DataSourceCombination> getDataSourceCombinations() {
        return Collections.unmodifiableSet(EnumSet.allOf(DataSourceCombination.class));
    }

    protected int getSkipFactor() {
        return 1;
    }

    public List<Arguments> getSummaryStatsParameters() {
        List<Arguments> result = new LinkedList<>();
        int skipFactor = this.getSkipFactor();
        Set<DataSourceCombination> sourceCombinations = this.getDataSourceCombinations();

        ThrowingConnectionProvider sqlExceptionProvider 
            = new ThrowingConnectionProvider(SQLException.class);

        ThrowingConnectionProvider nullPointerProvider
            = new ThrowingConnectionProvider(NullPointerException.class);
        
        this.summaryStatsMap.forEach((repoType, summaryStats) -> {
            ConnectionProvider connProvider = this.getConnectionProvider(repoType);

            Set<String> emptySet = Collections.emptySet();

            if (sourceCombinations.contains(LOADED)) {
                result.add(Arguments.of(
                    repoType, sqlExceptionProvider, "*", "*", LOADED, null, summaryStats, 
                    SQLException.class));
                result.add(Arguments.of(
                    repoType, nullPointerProvider, "*", "*", LOADED, null, summaryStats, 
                    NullPointerException.class));

                result.add(Arguments.of(
                    repoType, connProvider, "*", "*", LOADED, null, summaryStats, null));
                result.add(Arguments.of(
                    repoType, connProvider, "*", "*", LOADED, emptySet, summaryStats, null));
            }

            // get the set of all loaded data sources
            Repository  repo            = DataMartTestExtension.getRepository(repoType);
            Set<String> loadedSources   = repo.getLoadedDataSources();

            if (sourceCombinations.contains(LOADED)) {        
                result.add(Arguments.of(
                    repoType, connProvider, "*", "*", LOADED, loadedSources, summaryStats, null));
            }

            // figure out which data sources are configured with no loaded records
            Set<String> dataSources = repo.getConfiguredDataSources();

            // determine the unused sources
            Set<String> unusedSources = new TreeSet<>(dataSources);
            unusedSources.removeAll(loadedSources);
            unusedSources = Collections.unmodifiableSet(unusedSources);

            // determine the basic sources
            Set<String> basicUnused = new TreeSet<>(unusedSources);
            basicUnused.remove("TEST");
            basicUnused.remove("SEARCH");
            basicUnused = Collections.unmodifiableSet(basicUnused);
            
            // create all cross keys
            Set<CrossSourceKey> crossKeys = new TreeSet<>();
            for (String source1 : dataSources) {
                for (String source2 : dataSources) {
                    crossKeys.add(new CrossSourceKey(source1, source2));
                }
            }

            SzMatchCounts zeroMatches = new SzMatchCounts();
            SzRelationCounts zeroRelations = new SzRelationCounts();
            
            // create a deep clone of the original summary stats without the unused sources
            SzSummaryStats allStats = filter(
                summaryStats, loadedSources, unusedSources, "*", "*");
            
            // add the fake source summaries for unused sources versus loaded sources
            for (String source : unusedSources) {
                SzSourceSummary summary = new SzSourceSummary(source);

                for (String loaded : loadedSources) {
                    SzCrossSourceSummary cross = new SzCrossSourceSummary(source, loaded);
                    cross.addMatches(zeroMatches);
                    cross.addAmbiguousMatches(zeroRelations);
                    cross.addPossibleMatches(zeroRelations);
                    cross.addPossibleRelations(zeroRelations);
                    cross.addDisclosedRelations(zeroRelations);

                    summary.addCrossSourceSummary(cross);
                }

                allStats.addSourceSummary(summary);
            }

            // add the zero cross source summaries for unused sources to all source summaries
            for (SzSourceSummary summary : allStats.getSourceSummaries()) {
                // create cross summaries against the unused data sources
                for (String source : unusedSources) {
                    SzCrossSourceSummary cross = new SzCrossSourceSummary(summary.getDataSource(), source);
                    cross.addMatches(zeroMatches);
                    cross.addAmbiguousMatches(zeroRelations);
                    cross.addPossibleMatches(zeroRelations);
                    cross.addPossibleRelations(zeroRelations);
                    cross.addDisclosedRelations(zeroRelations);

                    summary.addCrossSourceSummary(cross);
                }
            }

            if (sourceCombinations.contains(ALL_BUT_DEFAULT)) {
                result.add(Arguments.of(
                    repoType, connProvider, null, null, ALL_BUT_DEFAULT, basicUnused,
                    filter(allStats, loadedSources, basicUnused, null, null),
                    null));
                
                result.add(Arguments.of(
                    repoType, connProvider, "*", "*", ALL_BUT_DEFAULT, basicUnused,
                    filter(allStats, loadedSources, basicUnused, "*", "*"),
                    null));
            }

            if (sourceCombinations.contains(ALL_WITH_DEFAULT)) {
                result.add(Arguments.of(
                    repoType, connProvider, "*", "*", ALL_WITH_DEFAULT, dataSources, allStats, null));
                result.add(Arguments.of(
                    repoType, connProvider, "*", "*", ALL_WITH_DEFAULT, unusedSources, allStats, null));
            }

            Map<String, Set<String>> matchKeyMap = repo.getRelatedMatchKeys();
            Map<String, Set<String>> principleMap = repo.getRelatedPrinciples();

            final Set<String> allUnusedSources = unusedSources;
            final Set<String> allBasicUnused = basicUnused;
            final int[] iteration = { 0 };
            // loop through the match key and principle options
            matchKeyMap.forEach((matchKey, principles) -> {
                if (((iteration[0]++) % skipFactor) != 0) {
                    return;        
                }
                
                if (sourceCombinations.contains(LOADED)) {
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, "*", LOADED, null,
                        filter(allStats, loadedSources, null, matchKey, "*"),
                        null));
                
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, null, LOADED, null,
                        filter(allStats, loadedSources, null, matchKey, null),
                        null));
                }

                if (sourceCombinations.contains(ALL_BUT_DEFAULT)) {
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, "*", ALL_BUT_DEFAULT, allBasicUnused,
                        filter(allStats, loadedSources, allBasicUnused, matchKey, "*"),
                        null));
                
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, null, ALL_BUT_DEFAULT, allBasicUnused,
                        filter(allStats, loadedSources, allBasicUnused, matchKey, null),
                        null));
                }

                if (sourceCombinations.contains(ALL_WITH_DEFAULT)) {
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, "*", ALL_WITH_DEFAULT, allUnusedSources,
                        filter(allStats, loadedSources, allUnusedSources, matchKey, "*"),
                        null));
                
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, null, ALL_WITH_DEFAULT, allUnusedSources,
                        filter(allStats, loadedSources, allUnusedSources, matchKey, null),
                        null));
                }

                principles.forEach(principle -> {
                    if (((iteration[0]++) % skipFactor) != 0) {
                        return;        
                    }
                    if (sourceCombinations.contains(LOADED)) {
                        result.add(Arguments.of(
                            repoType, connProvider, matchKey, principle, LOADED, null,
                            filter(allStats, loadedSources, null, matchKey, principle),
                            null));
                    }
                    
                    if (sourceCombinations.contains(ALL_BUT_DEFAULT)) {
                        result.add(Arguments.of(
                            repoType, connProvider, matchKey, principle, ALL_BUT_DEFAULT, allBasicUnused,
                            filter(allStats, loadedSources, allBasicUnused, matchKey, principle),
                            null));
                    }

                    if (sourceCombinations.contains(ALL_WITH_DEFAULT)) {
                        result.add(Arguments.of(
                            repoType, connProvider, matchKey, principle, ALL_WITH_DEFAULT, allUnusedSources,
                            filter(allStats, loadedSources, allUnusedSources, matchKey, principle),
                            null));
                    }
                });
            });

            // loop through the principle keys
            principleMap.keySet().forEach(principle -> {
                if (((iteration[0]++) % skipFactor) != 0) {
                    return;        
                }
                if (sourceCombinations.contains(LOADED)) {
                    result.add(Arguments.of(
                        repoType, connProvider, "*", principle, LOADED, null,
                        filter(allStats, loadedSources, null, "*", principle),
                        null));
                
                    result.add(Arguments.of(
                        repoType, connProvider, null, principle, LOADED, null,
                        filter(allStats, loadedSources, null, null, principle),
                        null));
                }

                if (sourceCombinations.contains(ALL_BUT_DEFAULT)) {
                    result.add(Arguments.of(
                        repoType, connProvider, "*", principle, ALL_BUT_DEFAULT, allBasicUnused,
                        filter(allStats, loadedSources, allBasicUnused, "*", principle),
                        null));
                    
                    result.add(Arguments.of(
                        repoType, connProvider, null, principle, ALL_BUT_DEFAULT, allBasicUnused,
                        filter(allStats, loadedSources, allBasicUnused, null, principle),
                        null));
                }

                if (sourceCombinations.contains(ALL_WITH_DEFAULT)) {
                    result.add(Arguments.of(
                        repoType, connProvider, "*", principle, ALL_WITH_DEFAULT, allUnusedSources,
                        filter(allStats, loadedSources, allUnusedSources, "*", principle),
                        null));
                    
                    result.add(Arguments.of(
                        repoType, connProvider, null, principle, ALL_WITH_DEFAULT, allUnusedSources,
                        filter(allStats, loadedSources, allUnusedSources, null, principle),
                        null));
                }
            });


            if (sourceCombinations.contains(COMPLEX)) {
                // check if we have more than one unused data source
                if (unusedSources.size() > 1) {
                    // make smaller sets of unused sources of various sizes
                    for (int count = 1; count < unusedSources.size(); count++) {
                        // setup the sources parameter and expected stats parameter
                        Iterator<String>    iter    = unusedSources.iterator();
                        Set<String>         sources = new TreeSet<>();

                        // now create a set of unused sources
                        for (int index = 0; index < count && iter.hasNext(); index++) {
                            sources.add(iter.next());
                        }

                        // make the source set unmodifiable
                        final Set<String> extraSources = Collections.unmodifiableSet(sources);

                        // add tests for all match keys and principles
                        result.add(Arguments.of(
                            repoType, connProvider, "*", "*", COMPLEX, extraSources, 
                            filter(allStats, loadedSources, extraSources, "*", "*"),
                            null));

                        result.add(Arguments.of(
                            repoType, connProvider, null, "*", COMPLEX, extraSources, 
                            filter(allStats, loadedSources, extraSources, null, "*"),
                            null));
                        
                        result.add(Arguments.of(
                            repoType, connProvider, "*", null, COMPLEX, extraSources, 
                            filter(allStats, loadedSources, extraSources, "*", null),
                            null));

                        result.add(Arguments.of(
                            repoType, connProvider, null, null, COMPLEX, extraSources, 
                            filter(allStats, loadedSources, extraSources, null, null),
                            null));

                        // loop through the match key and principle options
                        iteration[0] = 0;
                        matchKeyMap.forEach((matchKey, principles) -> {
                            if (((iteration[0]++) % skipFactor) != 0) {
                                return;        
                            }
                            result.add(Arguments.of(
                                repoType, connProvider, matchKey, "*", COMPLEX, extraSources,
                                filter(allStats, loadedSources, extraSources, matchKey, "*"),
                                null));
                            
                            result.add(Arguments.of(
                                repoType, connProvider, matchKey, null, COMPLEX, extraSources,
                                filter(allStats, loadedSources, extraSources, matchKey, null),
                                null));

                            principles.forEach(principle -> {
                                if (((iteration[0]++) % skipFactor) != 0) {
                                    return;        
                                }
                                result.add(Arguments.of(
                                    repoType, connProvider, matchKey, principle, COMPLEX, extraSources,
                                    filter(allStats, loadedSources, extraSources, matchKey, principle),
                                null));
                            });
                        });

                        // loop through the principle keys
                        principleMap.keySet().forEach(principle -> {
                            if (((iteration[0]++) % skipFactor) != 0) {
                                return;        
                            }
                        result.add(Arguments.of(
                                repoType, connProvider, "*", principle, COMPLEX, extraSources,
                                filter(allStats, loadedSources, extraSources, "*", principle),
                                null));
                            
                            result.add(Arguments.of(
                                repoType, connProvider, null, principle, COMPLEX, extraSources,
                                filter(allStats, loadedSources, extraSources, null, principle),
                                null));
                        });
                    }
                }
            }
        });

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<Arguments> getSourceSummaryParameters() {
        List<Arguments> result = new LinkedList<>();

        for (RepositoryType repoType : RepositoryType.values()) {
            result.add(Arguments.of(
                repoType, getConnectionProvider(repoType), null, null, null,
                LOADED, null, null, NullPointerException.class));
        }

        List<Arguments> summaryParams = this.getSummaryStatsParameters();
        for (Arguments origParams : summaryParams) {
            Object[]                paramArray      = origParams.get();
            RepositoryType          repoType        = (RepositoryType) paramArray[0];
            ConnectionProvider      connProvider    = (ConnectionProvider) paramArray[1];
            String                  matchKey        = (String) paramArray[2];
            String                  principle       = (String) paramArray[3];
            DataSourceCombination   sourceCombo     = (DataSourceCombination) paramArray[4];
            Set<String>             extraSources    = (Set<String>) paramArray[5];
            SzSummaryStats          summaryStats    = (SzSummaryStats) paramArray[6];
            Class<?>                exceptionType   = (Class<?>) paramArray[7];

            for (SzSourceSummary summary : summaryStats.getSourceSummaries()) {
                String dataSource = summary.getDataSource();
                result.add(Arguments.of(
                    repoType, connProvider, dataSource, matchKey, principle, 
                    sourceCombo, extraSources, summary, exceptionType));
            }
        }
        return result;
    }

    public List<Arguments> getCrossSummaryParameters() {
        List<Arguments> result = new LinkedList<>();

        for (RepositoryType repoType : RepositoryType.values()) {
            ConnectionProvider connProvider = getConnectionProvider(repoType);

            result.add(Arguments.of(
                    repoType, connProvider, null, null, 
                    null, null, null, NullPointerException.class));

            result.add(Arguments.of(
                    repoType, connProvider, "TEST", null, 
                    null, null, null, NullPointerException.class));

            result.add(Arguments.of(
                    repoType, connProvider, null, "TEST", 
                    null, null, null, NullPointerException.class));
        }

        List<Arguments> summaryParams = this.getSourceSummaryParameters();
        Set<List<?>>  accountedFor  = new HashSet<>();
        
        for (Arguments origParams : summaryParams) {
            Object[]            paramArray      = origParams.get();
            RepositoryType      repoType        = (RepositoryType) paramArray[0];
            ConnectionProvider  connProvider    = (ConnectionProvider) paramArray[1];
            String              dataSource      = (String) paramArray[2];
            String              matchKey        = (String) paramArray[3];
            String              principle       = (String) paramArray[4];
            // skip index 5 (DataSourceCombination), index 6 (Set<String> of data sources)
            SzSourceSummary     sourceSummary   = (SzSourceSummary) paramArray[7];
            Class<?>            exceptionType   = (Class<?>) paramArray[8];

            // skip those with a null source summary -- these are error conditions
            // and we handle those above
            if (sourceSummary == null) {
                continue;
            }

            List<Object> params = new ArrayList<>(5);
            params.add(repoType);
            params.add(dataSource);
            params.add(null);
            params.add(matchKey);
            params.add(principle);

            // skip all for which no extra sources are requested
            for (SzCrossSourceSummary cross : sourceSummary.getCrossSourceSummaries()) {
                String vsDataSource = cross.getVersusDataSource();

                params.set(2, vsDataSource);
                if (accountedFor.contains(params)) {
                    continue;
                }
                accountedFor.add(new ArrayList<>(params));

                result.add(Arguments.of(
                    repoType, connProvider, dataSource, vsDataSource, 
                    matchKey, principle, cross, exceptionType));
            }
        }
        return result;
    }

    @ParameterizedTest()
    @MethodSource("getSummaryStatsParameters")
    public void testSummaryStats(RepositoryType         repoType,
                                 ConnectionProvider     connProvider,
                                 String                 matchKey,
                                 String                 principle,
                                 DataSourceCombination  sourceCombo,
                                 Set<String>            dataSources,
                                 SzSummaryStats         expected,
                                 Class<?>               exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], matchKey=[ " + matchKey
            + " ], principle=[ " + principle + " ], dataSourceCombo[ "
            + sourceCombo + " ], dataSources=[ " + dataSources + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzSummaryStats actual = this.getSummaryStatistics(
                    repoType, connProvider, matchKey, principle, sourceCombo, dataSources);
                
                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }
                validateReport(expected, actual, testInfo);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }
    
    /**
     * Gets the {@link SzSummaryStats} for the specified parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param sourceCombo The {@link DataSourceCombination} for the test.
     * @param dataSources The set of data sources to include.
     * @return The {@link SzSummaryStats} result.
     * @throws Exception If an error occurs.
     */
    protected SzSummaryStats getSummaryStatistics(RepositoryType         repoType,
                                                  ConnectionProvider     connProvider,
                                                  String                 matchKey,
                                                  String                 principle,
                                                  DataSourceCombination  sourceCombo,
                                                  Set<String>            dataSources)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryStatistics(conn,
                                                           matchKey,
                                                           principle,
                                                           dataSources,
                                                           null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest()
    @MethodSource("getSourceSummaryParameters")
    public void testSourceSummary(RepositoryType        repoType,
                                  ConnectionProvider    connProvider,
                                  String                dataSource,
                                  String                matchKey,
                                  String                principle,
                                  DataSourceCombination sourceCombo,
                                  Set<String>           dataSources,
                                  SzSourceSummary       expected,
                                  Class<?>              exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], dataSourceCombo=[ " + sourceCombo
            + " ]. dataSources=[ " + dataSources + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzSourceSummary actual = this.getSourceSummary(
                    repoType, connProvider, dataSource, matchKey, principle, sourceCombo, dataSources);

                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }
                validateReport(expected, actual, testInfo);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzSourceSummary} for the specified data source.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The data source to get the summary for.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param sourceCombo The {@link DataSourceCombination} for the test.
     * @param dataSources The set of data sources to include.
     * @return The {@link SzSourceSummary} result.
     * @throws Exception If an error occurs.
     */
    protected SzSourceSummary getSourceSummary(RepositoryType        repoType,
                                               ConnectionProvider    connProvider,
                                               String                dataSource,
                                               String                matchKey,
                                               String                principle,
                                               DataSourceCombination sourceCombo,
                                               Set<String>           dataSources)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSourceSummary(conn,
                                                       dataSource,
                                                       matchKey,
                                                       principle,
                                                       dataSources,
                                                       null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest()
    @MethodSource("getCrossSummaryParameters")
    public void testCrossSummary(RepositoryType        repoType,
                                 ConnectionProvider    connProvider,
                                 String                dataSource,
                                 String                vsDataSource,
                                 String                matchKey,
                                 String                principle,
                                 SzCrossSourceSummary  expected,
                                 Class<?>              exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            SzCrossSourceMatchCounts matchCounts = null;
            SzCrossSourceRelationCounts relationCounts = null;            
            try {
                SzCrossSourceSummary actual = this.getCrossSourceSummary(
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle);
                
                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }

                validateReport(expected, actual, testInfo);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }

            try {
                matchCounts = this.getCrossSourceMatchSummary(
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle);

                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }

                validateReport(expected.getMatches(),
                            matchCounts.getCounts(),
                            "CrossSourceMatchSummary",
                            testInfo);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }

            try {
                relationCounts = this.getCrossSourceAmbiguousMatchSummary(
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle);

                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }

                validateReport(expected.getAmbiguousMatches(),
                            relationCounts.getCounts(),
                            "CrossSourceAmbiguousMatchSummary",
                            testInfo);


            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }

            try {
                relationCounts = this.getCrossSourcePossibleMatchSummary(
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle);

                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }

                validateReport(expected.getPossibleMatches(),
                            relationCounts.getCounts(),
                            "CrossSourcePossibleMatchSummary",
                            testInfo);


            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }

            try {
                relationCounts = this.getCrossSourcePossibleRelationSummary(
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle);

                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }

                validateReport(expected.getPossibleRelations(),
                            relationCounts.getCounts(),
                            "CrossSourcePossibleRelationSummary",
                            testInfo);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }

            try {
                relationCounts = this.getCrossSourceDisclosedRelationSummary(
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle);

                if (exceptionType != null) {
                    fail("Unexpectedly succeeded when expecting an exception (" 
                        + exceptionType + "): " + testInfo);
                }

                validateReport(expected.getDisclosedRelations(),
                            relationCounts.getCounts(),
                            "CrossSourceDisclosedRelationSummary",
                            testInfo);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzCrossSourceSummary} for the specified data sources.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @return The {@link SzCrossSourceSummary} result.
     * @throws Exception If an error occurs.
     */
    protected SzCrossSourceSummary getCrossSourceSummary(RepositoryType     repoType,
                                                         ConnectionProvider connProvider,
                                                         String             dataSource,
                                                         String             vsDataSource,
                                                         String             matchKey,
                                                         String             principle)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossSourceSummary(conn,
                                                            dataSource,
                                                            vsDataSource,
                                                            matchKey,
                                                            principle,
                                                            null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    /**
     * Gets the {@link SzCrossSourceMatchCounts} for the specified data sources.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @return The {@link SzCrossSourceMatchCounts} result.
     * @throws Exception If an error occurs.
     */
    protected SzCrossSourceMatchCounts getCrossSourceMatchSummary(RepositoryType     repoType,
                                                                  ConnectionProvider connProvider,
                                                                  String             dataSource,
                                                                  String             vsDataSource,
                                                                  String             matchKey,
                                                                  String             principle)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossSourceMatchSummary(
                conn, dataSource, vsDataSource, matchKey, principle, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    /**
     * Gets the {@link SzCrossSourceRelationCounts} for ambiguous matches.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @return The {@link SzCrossSourceRelationCounts} result.
     * @throws Exception If an error occurs.
     */
    protected SzCrossSourceRelationCounts getCrossSourceAmbiguousMatchSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossSourceAmbiguousMatchSummary(
                conn, dataSource, vsDataSource, matchKey, principle, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    /**
     * Gets the {@link SzCrossSourceRelationCounts} for possible matches.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @return The {@link SzCrossSourceRelationCounts} result.
     * @throws Exception If an error occurs.
     */
    protected SzCrossSourceRelationCounts getCrossSourcePossibleMatchSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossSourcePossibleMatchSummary(
                conn, dataSource, vsDataSource, matchKey, principle, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    /**
     * Gets the {@link SzCrossSourceRelationCounts} for possible relations.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @return The {@link SzCrossSourceRelationCounts} result.
     * @throws Exception If an error occurs.
     */
    protected SzCrossSourceRelationCounts getCrossSourcePossibleRelationSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossSourcePossibleRelationSummary(
                conn, dataSource, vsDataSource, matchKey, principle, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    /**
     * Gets the {@link SzCrossSourceRelationCounts} for disclosed relations.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @return The {@link SzCrossSourceRelationCounts} result.
     * @throws Exception If an error occurs.
     */
    protected SzCrossSourceRelationCounts getCrossSourceDisclosedRelationSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossSourceDisclosedRelationSummary(
                conn, dataSource, vsDataSource, matchKey, principle, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    public static interface EntityQualifier {
        public boolean test(SzResolvedEntity entity,
                            String           dataSource, 
                            String           vsDataSource,
                            String           matchKey,
                            String           principle);
    }

    public static interface RelationQualifier {
        public boolean test(RelationPair    relationPair,
                            String          dataSource, 
                            String          vsDataSource,
                            String          matchKey,
                            String          principle);
    }

    public List<Arguments> getSummaryEntitiesParameters(EntityQualifier qualifier) {
        List<Arguments> result = new LinkedList<>();
        int skipFactor = this.getSkipFactor();

        ThrowingConnectionProvider sqlExceptionProvider 
            = new ThrowingConnectionProvider(SQLException.class);

        for (RepositoryType repoType : RepositoryType.values()) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            result.add(Arguments.of(repoType,
                                    sqlExceptionProvider,
                                    "TEST",
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    SQLException.class));

            result.add(Arguments.of(repoType,
                                    this.getConnectionProvider(repoType),
                                    null,
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    NullPointerException.class));

            Map<String, Set<String>> matchKeyMap = repo.getRelatedMatchKeys();
            Map<String, Set<String>> principleMap = repo.getRelatedPrinciples();
            Set<MatchPairKey> matchPairs = new TreeSet<>();
            matchPairs.add(new MatchPairKey("*", "*"));    
            matchPairs.add(new MatchPairKey("*", null));    
            matchPairs.add(new MatchPairKey(null, "*"));    
            matchPairs.add(new MatchPairKey(null, null)); 
            matchKeyMap.forEach((matchKey, principles) -> {
                matchPairs.add(new MatchPairKey(matchKey, null));
                principles.forEach(principle -> {
                    matchPairs.add(new MatchPairKey(null, principle));
                });
            });
            principleMap.forEach((principle, matchKeys) -> {
                matchPairs.add(new MatchPairKey(null, principle));
            });

            Set<String> dataSources = repo.getConfiguredDataSources();

            for (String dataSource : dataSources) {
                int skipIndex = 0;
                for (MatchPairKey matchPair : matchPairs) {
                    String matchKey = matchPair.getMatchKey();
                    String principle = matchPair.getPrinciple();
                    if ((matchKey != null && !"*".equals(matchKey))
                        || (principle !=null && !"*".equals(principle)))
                    {
                        if (skipIndex++ % skipFactor != 0) {
                            continue;
                        }
                    }

                    List<SzEntitiesPageParameters> params 
                        = this.generateEntitiesPageParameters(
                            repoType, 
                            (e) -> qualifier.test(
                                e,
                                dataSource,
                                dataSource,
                                matchPair.getMatchKey(),
                                matchPair.getPrinciple()));
                            
                    for (SzEntitiesPageParameters p : params) {
                        result.add(Arguments.of(repoType,
                                                this.getConnectionProvider(repoType),
                                                dataSource,
                                                matchPair.getMatchKey(),
                                                matchPair.getPrinciple(),
                                                p.getEntityIdBound(),
                                                p.getBoundType(),
                                                p.getPageSize(),
                                                p.getSampleSize(),
                                                p.getExpectedPage(),
                                                null));
                    }
                }
            }
        }
        return result;
    }

    public List<Arguments> getSummaryMatchEntityParameters() {
        return this.getSummaryEntitiesParameters(
            (entity, dataSource, vsDataSource, matchKey, principle) -> {
                Map<String, Integer> recordSummary = entity.getSourceSummary();
                if (!recordSummary.containsKey(dataSource)
                    || recordSummary.get(dataSource) == 1)
                {
                    return false;
                }
                for (SzRecord record : entity.getRecords().values()) {
                    // check if this record has expected match key or principle
                    if ((matchKey == null || "*".equals(matchKey) 
                        || matchKey.equals(record.getMatchKey()))
                        && (principle == null || "*".equals(principle)
                            || principle.equals(record.getPrinciple())))
                    {
                        return true;
                    }
                }
                return false;
        });
    }

    public List<Arguments> getSummaryRelatedEntityParameters(SzMatchType matchType) {
        return this.getSummaryEntitiesParameters(
            (entity, dataSource, vsDataSource, matchKey, principle) -> 
            {
                Map<String, Integer> recordSummary = entity.getSourceSummary();
                if (!recordSummary.containsKey(dataSource)) {
                    return false;
                }
                for (SzRelatedEntity related : entity.getRelatedEntities().values()) 
                {
                    // check if this related entity has the right match type
                    if (related.getMatchType() != matchType) {
                        continue;
                    }

                    // check if this related entity has the expected data source
                    Map<String, Integer> relatedSummary = related.getSourceSummary();
                    if (!relatedSummary.containsKey(vsDataSource)) {
                        continue;
                    }

                    String revMatchKey = SzRelationship.getReverseMatchKey(related.getMatchKey());

                    // check if the related entity has the match key and principle
                    if ((matchKey == null || "*".equals(matchKey) 
                        || matchKey.equals(related.getMatchKey()) 
                        || matchKey.equals(revMatchKey))
                        && (principle == null || "*".equals(principle)
                            || principle.equals(related.getPrinciple())))
                    {
                        return true;
                    }
                }

                // if we get here then return false
                return false;
            });
    }

    public List<Arguments> getSummaryAmbiguousMatchEntityParameters() {
        return getSummaryRelatedEntityParameters(AMBIGUOUS_MATCH);
    }

    public List<Arguments> getSummaryPossibleMatchEntityParameters() {
        return getSummaryRelatedEntityParameters(POSSIBLE_MATCH);
    }

    public List<Arguments> getSummaryPossibleRelationEntityParameters() {
        return getSummaryRelatedEntityParameters(POSSIBLE_RELATION);
    }

    public List<Arguments> getSummaryDisclosedRelationEntityParameters() {
        return getSummaryRelatedEntityParameters(DISCLOSED_RELATION);
    }

    public List<Arguments> getCrossEntitiesParameters(EntityQualifier qualifier) {
        List<Arguments> result = new LinkedList<>();
        int skipFactor = this.getSkipFactor();

        ThrowingConnectionProvider sqlExceptionProvider 
            = new ThrowingConnectionProvider(SQLException.class);

        for (RepositoryType repoType : RepositoryType.values()) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            result.add(Arguments.of(repoType,
                                    sqlExceptionProvider,
                                    "TEST",
                                    "TEST",
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    SQLException.class));
    
            result.add(Arguments.of(repoType,
                                    this.getConnectionProvider(repoType),
                                    null,
                                    "TEST",
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    NullPointerException.class));
            
            result.add(Arguments.of(repoType,
                                    this.getConnectionProvider(repoType),
                                    null,
                                    null,
                                    "TEST",
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    NullPointerException.class));
            
            Map<String, Set<String>> matchKeyMap = repo.getRelatedMatchKeys();
            Map<String, Set<String>> principleMap = repo.getRelatedPrinciples();
            Set<MatchPairKey> matchPairs = new TreeSet<>();
            matchPairs.add(new MatchPairKey("*", "*"));    
            matchPairs.add(new MatchPairKey("*", null));    
            matchPairs.add(new MatchPairKey(null, "*"));    
            matchPairs.add(new MatchPairKey(null, null)); 
            matchKeyMap.forEach((matchKey, principles) -> {
                matchPairs.add(new MatchPairKey(matchKey, null));
                principles.forEach(principle -> {
                    matchPairs.add(new MatchPairKey(null, principle));
                });
            });
            principleMap.forEach((principle, matchKeys) -> {
                matchPairs.add(new MatchPairKey(null, principle));
            });

            Set<String> dataSources = repo.getConfiguredDataSources();

            for (String dataSource : dataSources) {
                for (String vsDataSource : dataSources) {
                    int skipIndex = 0;
                    for (MatchPairKey matchPair : matchPairs) {
                        String matchKey = matchPair.getMatchKey();
                        String principle = matchPair.getPrinciple();
                        if ((matchKey != null && !"*".equals(matchKey))
                            || (principle !=null && !"*".equals(principle)))
                        {
                            if (skipIndex++ % skipFactor != 0) {
                                continue;
                            }
                        }

                        List<SzEntitiesPageParameters> params 
                            = this.generateEntitiesPageParameters(
                                repoType, 
                                (e) -> qualifier.test(
                                    e,
                                    dataSource,
                                    vsDataSource,
                                    matchPair.getMatchKey(),
                                    matchPair.getPrinciple()));
                                
                        for (SzEntitiesPageParameters p : params) {
                            result.add(Arguments.of(repoType,
                                                    this.getConnectionProvider(repoType),
                                                    dataSource,
                                                    vsDataSource,
                                                    matchPair.getMatchKey(),
                                                    matchPair.getPrinciple(),
                                                    p.getEntityIdBound(),
                                                    p.getBoundType(),
                                                    p.getPageSize(),
                                                    p.getSampleSize(),
                                                    p.getExpectedPage(),
                                                    null));
                        }
                    }
                }
            }
        }
        return result;
    }

    public List<Arguments> getCrossMatchEntityParameters() {
        return this.getCrossEntitiesParameters(
            (entity, dataSource, vsDataSource, matchKey, principle) -> {
                Map<String, Integer> recordSummary = entity.getSourceSummary();
                if (!recordSummary.containsKey(dataSource)) {
                    return false;
                }
                if (dataSource.equals(vsDataSource)
                    && recordSummary.get(dataSource) < 2)
                {
                    return false;
                }
                if (!recordSummary.containsKey(vsDataSource)) {
                    return false;
                }
                for (SzRecord record : entity.getRecords().values()) {
                    // check if this record has expected match key or principle
                    if ((matchKey == null || "*".equals(matchKey) 
                        || matchKey.equals(record.getMatchKey()))
                        && (principle == null || "*".equals(principle)
                            || principle.equals(record.getPrinciple())))
                    {
                        return true;
                    }
                }
                return false;
        });
    }

    public List<Arguments> getCrossRelatedEntityParameters(SzMatchType matchType) {
        return this.getCrossEntitiesParameters(
            (entity, dataSource, vsDataSource, matchKey, principle) -> 
            {
                Map<String, Integer> recordSummary = entity.getSourceSummary();
                if (!recordSummary.containsKey(dataSource)) {
                    return false;
                }
                for (SzRelatedEntity related : entity.getRelatedEntities().values()) 
                {
                    // check if this related entity has the right match type
                    if (related.getMatchType() != matchType) {
                        continue;
                    }

                    // check if this related entity has the expected data source
                    Map<String, Integer> relatedSummary = related.getSourceSummary();
                    if (!relatedSummary.containsKey(vsDataSource)) {
                        continue;
                    }

                    String revMatchKey = SzRelationship.getReverseMatchKey(related.getMatchKey());

                    // check if the related entity has the match key and principle
                    if ((matchKey == null || "*".equals(matchKey) 
                        || matchKey.equals(related.getMatchKey()) 
                        || matchKey.equals(revMatchKey))
                        && (principle == null || "*".equals(principle)
                            || principle.equals(related.getPrinciple())))
                    {
                        return true;
                    }
                }

                // if we get here then return false
                return false;
            });
    }

    public List<Arguments> getCrossAmbiguousMatchEntityParameters() {
        return getCrossRelatedEntityParameters(AMBIGUOUS_MATCH);
    }

    public List<Arguments> getCrossPossibleMatchEntityParameters() {
        return getCrossRelatedEntityParameters(POSSIBLE_MATCH);
    }

    public List<Arguments> getCrossPossibleRelationEntityParameters() {
        return getCrossRelatedEntityParameters(POSSIBLE_RELATION);
    }

    public List<Arguments> getCrossDisclosedRelationEntityParameters() {
        return getCrossRelatedEntityParameters(DISCLOSED_RELATION);
    }

    @ParameterizedTest
    @MethodSource("getSummaryMatchEntityParameters")
    public void testSummaryMatchEntities(RepositoryType     repoType,
                                         ConnectionProvider connProvider,
                                         String             dataSource,
                                         String             matchKey,
                                         String             principle,
                                         String             entityIdBound,
                                         SzBoundType        boundType,
                                         Integer            pageSize,
                                         Integer            sampleSize,
                                         SzEntitiesPage     expected,
                                         Class<?>           exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getSummaryMatchEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of match entity IDs for the specified summary parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The data source filter, or {@code null} for all data sources.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getSummaryMatchEntityIds(RepositoryType     repoType,
                                                      ConnectionProvider connProvider,
                                                      String             dataSource,
                                                      String             matchKey,
                                                      String             principle,
                                                      String             entityIdBound,
                                                      SzBoundType        boundType,
                                                      Integer            pageSize,
                                                      Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryMatchEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getSummaryAmbiguousMatchEntityParameters")
    public void testSummaryAmbiguousMatchEntities(RepositoryType     repoType,
                                                  ConnectionProvider connProvider,
                                                  String             dataSource,
                                                  String             matchKey,
                                                  String             principle,
                                                  String             entityIdBound,
                                                  SzBoundType        boundType,
                                                  Integer            pageSize,
                                                  Integer            sampleSize,
                                                  SzEntitiesPage     expected,
                                                  Class<?>           exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getSummaryAmbiguousMatchEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of ambiguous match entity IDs for the specified summary parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The data source filter, or {@code null} for all data sources.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getSummaryAmbiguousMatchEntityIds(RepositoryType     repoType,
                                                               ConnectionProvider connProvider,
                                                               String             dataSource,
                                                               String             matchKey,
                                                               String             principle,
                                                               String             entityIdBound,
                                                               SzBoundType        boundType,
                                                               Integer            pageSize,
                                                               Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryAmbiguousMatchEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getSummaryPossibleMatchEntityParameters")
    public void testSummaryPossibleMatchEntities(RepositoryType     repoType,
                                                 ConnectionProvider connProvider,
                                                 String             dataSource,
                                                 String             matchKey,
                                                 String             principle,
                                                 String             entityIdBound,
                                                 SzBoundType        boundType,
                                                 Integer            pageSize,
                                                 Integer            sampleSize,
                                                 SzEntitiesPage     expected,
                                                 Class<?>           exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getSummaryPossibleMatchEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of possible match entity IDs for the specified summary parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The data source filter, or {@code null} for all data sources.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getSummaryPossibleMatchEntityIds(RepositoryType     repoType,
                                                              ConnectionProvider connProvider,
                                                              String             dataSource,
                                                              String             matchKey,
                                                              String             principle,
                                                              String             entityIdBound,
                                                              SzBoundType        boundType,
                                                              Integer            pageSize,
                                                              Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryPossibleMatchEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getSummaryPossibleRelationEntityParameters")
    public void testSummaryPossibleRelationEntities(RepositoryType     repoType,
                                                    ConnectionProvider connProvider,
                                                    String             dataSource,
                                                    String             matchKey,
                                                    String             principle,
                                                    String             entityIdBound,
                                                    SzBoundType        boundType,
                                                    Integer            pageSize,
                                                    Integer            sampleSize,
                                                    SzEntitiesPage     expected,
                                                    Class<?>           exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getSummaryPossibleRelationEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of possible relation entity IDs for the specified summary parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The data source filter, or {@code null} for all data sources.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getSummaryPossibleRelationEntityIds(RepositoryType     repoType,
                                                                 ConnectionProvider connProvider,
                                                                 String             dataSource,
                                                                 String             matchKey,
                                                                 String             principle,
                                                                 String             entityIdBound,
                                                                 SzBoundType        boundType,
                                                                 Integer            pageSize,
                                                                 Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryPossibleRelationEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getSummaryDisclosedRelationEntityParameters")
    public void testSummaryDisclosedRelationEntities(RepositoryType     repoType,
                                                    ConnectionProvider connProvider,
                                                    String             dataSource,
                                                    String             matchKey,
                                                    String             principle,
                                                    String             entityIdBound,
                                                    SzBoundType        boundType,
                                                    Integer            pageSize,
                                                    Integer            sampleSize,
                                                    SzEntitiesPage     expected,
                                                    Class<?>           exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getSummaryDisclosedRelationEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of disclosed relation entity IDs for the specified summary parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The data source filter, or {@code null} for all data sources.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getSummaryDisclosedRelationEntityIds(RepositoryType     repoType,
                                                                  ConnectionProvider connProvider,
                                                                  String             dataSource,
                                                                  String             matchKey,
                                                                  String             principle,
                                                                  String             entityIdBound,
                                                                  SzBoundType        boundType,
                                                                  Integer            pageSize,
                                                                  Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryDisclosedRelationEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossMatchEntityParameters")
    public void testCrossMatchEntities(RepositoryType       repoType,
                                       ConnectionProvider   connProvider,
                                       String               dataSource,
                                       String               vsDataSource,
                                       String               matchKey,
                                       String               principle,
                                       String               entityIdBound,
                                       SzBoundType          boundType,
                                       Integer              pageSize,
                                       Integer              sampleSize,
                                       SzEntitiesPage       expected,
                                       Class<?>             exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getCrossMatchEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of match entity IDs for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getCrossMatchEntityIds(RepositoryType       repoType,
                                                    ConnectionProvider   connProvider,
                                                    String               dataSource,
                                                    String               vsDataSource,
                                                    String               matchKey,
                                                    String               principle,
                                                    String               entityIdBound,
                                                    SzBoundType          boundType,
                                                    Integer              pageSize,
                                                    Integer              sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossMatchEntityIds(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossAmbiguousMatchEntityParameters")
    public void testCrossAmbiguousMatchEntities(RepositoryType      repoType,
                                                ConnectionProvider  connProvider,
                                                String              dataSource,
                                                String              vsDataSource,
                                                String              matchKey,
                                                String              principle,
                                                String              entityIdBound,
                                                SzBoundType         boundType,
                                                Integer             pageSize,
                                                Integer             sampleSize,
                                                SzEntitiesPage      expected,
                                                Class<?>            exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getCrossAmbiguousMatchEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of ambiguous match entity IDs for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getCrossAmbiguousMatchEntityIds(RepositoryType      repoType,
                                                             ConnectionProvider  connProvider,
                                                             String              dataSource,
                                                             String              vsDataSource,
                                                             String              matchKey,
                                                             String              principle,
                                                             String              entityIdBound,
                                                             SzBoundType         boundType,
                                                             Integer             pageSize,
                                                             Integer             sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossAmbiguousMatchEntityIds(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossPossibleMatchEntityParameters")
    public void testCrossPossibleMatchEntities(RepositoryType       repoType,
                                               ConnectionProvider   connProvider,
                                               String               dataSource,
                                               String               vsDataSource,
                                               String               matchKey,
                                               String               principle,
                                               String               entityIdBound,
                                               SzBoundType          boundType,
                                               Integer              pageSize,
                                               Integer              sampleSize,
                                               SzEntitiesPage       expected,
                                               Class<?>             exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getCrossPossibleMatchEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of possible match entity IDs for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getCrossPossibleMatchEntityIds(RepositoryType       repoType,
                                                            ConnectionProvider   connProvider,
                                                            String               dataSource,
                                                            String               vsDataSource,
                                                            String               matchKey,
                                                            String               principle,
                                                            String               entityIdBound,
                                                            SzBoundType          boundType,
                                                            Integer              pageSize,
                                                            Integer              sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossPossibleMatchEntityIds(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossPossibleRelationEntityParameters")
    public void testCrossPossibleRelationEntities(RepositoryType        repoType,
                                                  ConnectionProvider    connProvider,
                                                  String                dataSource,
                                                  String                vsDataSource,
                                                  String                matchKey,
                                                  String                principle,
                                                  String                entityIdBound,
                                                  SzBoundType           boundType,
                                                  Integer               pageSize,
                                                  Integer               sampleSize,
                                                  SzEntitiesPage        expected,
                                                  Class<?>              exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getCrossPossibleRelationEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of possible relation entity IDs for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getCrossPossibleRelationEntityIds(RepositoryType        repoType,
                                                               ConnectionProvider    connProvider,
                                                               String                dataSource,
                                                               String                vsDataSource,
                                                               String                matchKey,
                                                               String                principle,
                                                               String                entityIdBound,
                                                               SzBoundType           boundType,
                                                               Integer               pageSize,
                                                               Integer               sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossPossibleRelationEntityIds(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossDisclosedRelationEntityParameters")
    public void testCrossDisclosedRelationEntities(RepositoryType       repoType,
                                                   ConnectionProvider   connProvider,
                                                   String               dataSource,
                                                   String               vsDataSource,
                                                   String               matchKey,
                                                   String               principle,
                                                   String               entityIdBound,
                                                   SzBoundType          boundType,
                                                   Integer              pageSize,
                                                   Integer              sampleSize,
                                                   SzEntitiesPage       expected,
                                                   Class<?>             exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzEntitiesPage actual = this.getCrossDisclosedRelationEntityIds(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    entityIdBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateEntitiesPage(repoType,
                                          testInfo,
                                          entityIdBound,
                                          boundType,
                                          pageSize,
                                          sampleSize,
                                          expected,
                                          actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzEntitiesPage} of disclosed relation entity IDs for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param entityIdBound The entity ID boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getCrossDisclosedRelationEntityIds(RepositoryType       repoType,
                                                                ConnectionProvider   connProvider,
                                                                String               dataSource,
                                                                String               vsDataSource,
                                                                String               matchKey,
                                                                String               principle,
                                                                String               entityIdBound,
                                                                SzBoundType          boundType,
                                                                Integer              pageSize,
                                                                Integer              sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getCrossDisclosedRelationEntityIds(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    public List<Arguments> getCrossRelationParameters(RelationQualifier qualifier) {
        List<Arguments> result = new LinkedList<>();
        int skipFactor = this.getSkipFactor();

        ThrowingConnectionProvider sqlExceptionProvider 
            = new ThrowingConnectionProvider(SQLException.class);
        
        for (RepositoryType repoType : RepositoryType.values()) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            result.add(Arguments.of(repoType,
                                    sqlExceptionProvider,
                                    "TEST",
                                    "TEST",
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    SQLException.class));

            result.add(Arguments.of(repoType,
                                    this.getConnectionProvider(repoType),
                                    null,
                                    "TEST",
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    NullPointerException.class));

            result.add(Arguments.of(repoType,
                                    this.getConnectionProvider(repoType),
                                    "TEST",
                                    null,
                                    null,
                                    null,
                                    null,
                                    EXCLUSIVE_LOWER,
                                    100,
                                    null,
                                    null,
                                    NullPointerException.class));

            Map<String, Set<String>> matchKeyMap = repo.getRelatedMatchKeys();
            Map<String, Set<String>> principleMap = repo.getRelatedPrinciples();
            Set<MatchPairKey> matchPairs = new TreeSet<>();
            matchPairs.add(new MatchPairKey("*", "*"));    
            matchPairs.add(new MatchPairKey("*", null));    
            matchPairs.add(new MatchPairKey(null, "*"));    
            matchPairs.add(new MatchPairKey(null, null)); 
            matchKeyMap.forEach((matchKey, principles) -> {
                matchPairs.add(new MatchPairKey(matchKey, null));
                principles.forEach(principle -> {
                    matchPairs.add(new MatchPairKey(null, principle));
                });
            });
            principleMap.forEach((principle, matchKeys) -> {
                matchPairs.add(new MatchPairKey(null, principle));
            });

            Set<String> dataSources = repo.getConfiguredDataSources();

            for (String dataSource : dataSources) {
                for (String vsDataSource : dataSources) {
                    int skipIndex = 0;
                    for (MatchPairKey matchPair : matchPairs) {
                        String matchKey = matchPair.getMatchKey();
                        String principle = matchPair.getPrinciple();
                        if ((matchKey != null && !"*".equals(matchKey))
                            || (principle !=null && !"*".equals(principle)))
                        {
                            if (skipIndex++ % skipFactor != 0) {
                                continue;
                            }
                        }

                        List<SzRelationsPageParameters> params 
                            = this.generateRelationsPageParameters(
                                repoType, 
                                (relationPair) -> qualifier.test(
                                    relationPair,
                                    dataSource,
                                    vsDataSource,
                                    matchPair.getMatchKey(),
                                    matchPair.getPrinciple()));
                                
                        for (SzRelationsPageParameters p : params) {
                            result.add(Arguments.of(repoType,
                                                    this.getConnectionProvider(repoType),
                                                    dataSource,
                                                    vsDataSource,
                                                    matchPair.getMatchKey(),
                                                    matchPair.getPrinciple(),
                                                    p.getRelationBound(),
                                                    p.getBoundType(),
                                                    p.getPageSize(),
                                                    p.getSampleSize(),
                                                    p.getExpectedPage(),
                                                    null));
                        }
                    }
                }
            }
        }
        return result;
    }

    public List<Arguments> getCrossRelationParameters(SzMatchType matchType) {
        return this.getCrossRelationParameters(
            (relationPair, dataSource, vsDataSource, matchKey, principle) -> 
            {
                SzResolvedEntity    entity      = relationPair.entity();
                SzRelatedEntity     related     = relationPair.related();
                SzResolvedEntity    resolvedRel = relationPair.resolvedRelated();

                // sanity checks that the related entity is related to the main entity
                if (!entity.getRelatedEntities().containsKey(related.getEntityId())) {
                    return false;
                }
                if (related.getEntityId() != resolvedRel.getEntityId()) {
                    return false;
                }
                if (!resolvedRel.getRelatedEntities().containsKey(entity.getEntityId())) {
                    return false;
                }

                // check the match type for the related entity
                if (related.getMatchType() != matchType) {
                    return false;
                }

                // if resolved entity does not hae the target data source, return false
                if (!entity.getSourceSummary().containsKey(dataSource)) {
                    return false;
                }

                // now check the related entity for the data source
                if (!related.getSourceSummary().containsKey(vsDataSource)) {
                    return false;
                }
                
                String revMatchKey = resolvedRel.getRelatedEntities().get(
                    entity.getEntityId()).getMatchKey();
                
                // check if the related entity has the match key and principle
                return ((matchKey == null || "*".equals(matchKey) 
                    || matchKey.equals(related.getMatchKey()) || matchKey.equals(revMatchKey))
                    && (principle == null || "*".equals(principle)
                        || principle.equals(related.getPrinciple())));
            });
    }

    public List<Arguments> getCrossAmbiguousMatchParameters() {
        return getCrossRelationParameters(AMBIGUOUS_MATCH);
    }

    public List<Arguments> getCrossPossibleMatchParameters() {
        return getCrossRelationParameters(POSSIBLE_MATCH);
    }

    public List<Arguments> getCrossPossibleRelationParameters() {
        return getCrossRelationParameters(POSSIBLE_RELATION);
    }

    public List<Arguments> getCrossDisclosedRelationParameters() {
        return getCrossRelationParameters(DISCLOSED_RELATION);
    }

    @ParameterizedTest
    @MethodSource("getCrossAmbiguousMatchParameters")
    public void testCrossAmbiguousMatches(RepositoryType        repoType,
                                          ConnectionProvider    connProvider,
                                          String                dataSource,
                                          String                vsDataSource,
                                          String                matchKey,
                                          String                principle,
                                          String                relationBound,
                                          SzBoundType           boundType,
                                          Integer               pageSize,
                                          Integer               sampleSize,
                                          SzRelationsPage       expected,
                                          Class<?>              exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], relationBound=[ " + relationBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzRelationsPage actual = this.getSummaryAmbiguousMatches(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    relationBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateRelationsPage(repoType,
                                           testInfo,
                                           relationBound,
                                           boundType,
                                           pageSize,
                                           sampleSize,
                                           expected,
                                           actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzRelationsPage} of ambiguous matches for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param relationBound The relation boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzRelationsPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzRelationsPage getSummaryAmbiguousMatches(RepositoryType        repoType,
                                                         ConnectionProvider    connProvider,
                                                         String                dataSource,
                                                         String                vsDataSource,
                                                         String                matchKey,
                                                         String                principle,
                                                         String                relationBound,
                                                         SzBoundType           boundType,
                                                         Integer               pageSize,
                                                         Integer               sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryAmbiguousMatches(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                relationBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }


    @ParameterizedTest
    @MethodSource("getCrossPossibleMatchParameters")
    public void testCrossPossibleMatches(RepositoryType     repoType,
                                         ConnectionProvider connProvider,
                                         String             dataSource,
                                         String             vsDataSource,
                                         String             matchKey,
                                         String             principle,
                                         String             relationBound,
                                         SzBoundType        boundType,
                                         Integer            pageSize,
                                         Integer            sampleSize,
                                         SzRelationsPage    expected,
                                         Class<?>           exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], relationBound=[ " + relationBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzRelationsPage actual = this.getSummaryPossibleMatches(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    relationBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateRelationsPage(repoType,
                                           testInfo,
                                           relationBound,
                                           boundType,
                                           pageSize,
                                           sampleSize,
                                           expected,
                                           actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzRelationsPage} of possible matches for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param relationBound The relation boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzRelationsPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzRelationsPage getSummaryPossibleMatches(RepositoryType     repoType,
                                                        ConnectionProvider connProvider,
                                                        String             dataSource,
                                                        String             vsDataSource,
                                                        String             matchKey,
                                                        String             principle,
                                                        String             relationBound,
                                                        SzBoundType        boundType,
                                                        Integer            pageSize,
                                                        Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryPossibleMatches(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                relationBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossPossibleRelationParameters")
    public void testCrossPossibleRelations(RepositoryType       repoType,
                                           ConnectionProvider   connProvider,
                                           String               dataSource,
                                           String               vsDataSource,
                                           String               matchKey,
                                           String               principle,
                                           String               relationBound,
                                           SzBoundType          boundType,
                                           Integer              pageSize,
                                           Integer              sampleSize,
                                           SzRelationsPage      expected,
                                           Class<?>             exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], relationBound=[ " + relationBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzRelationsPage actual = this.getSummaryPossibleRelations(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    relationBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateRelationsPage(repoType,
                                           testInfo,
                                           relationBound,
                                           boundType,
                                           pageSize,
                                           sampleSize,
                                           expected,
                                           actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzRelationsPage} of possible relations for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param relationBound The relation boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzRelationsPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzRelationsPage getSummaryPossibleRelations(RepositoryType       repoType,
                                                          ConnectionProvider   connProvider,
                                                          String               dataSource,
                                                          String               vsDataSource,
                                                          String               matchKey,
                                                          String               principle,
                                                          String               relationBound,
                                                          SzBoundType          boundType,
                                                          Integer              pageSize,
                                                          Integer              sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryPossibleRelations(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                relationBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    @ParameterizedTest
    @MethodSource("getCrossDisclosedRelationParameters")
    public void testCrossDisclosedRelations(RepositoryType      repoType,
                                            ConnectionProvider  connProvider,
                                            String              dataSource,
                                            String              vsDataSource,
                                            String              matchKey,
                                            String              principle,
                                            String              relationBound,
                                            SzBoundType         boundType,
                                            Integer             pageSize,
                                            Integer             sampleSize,
                                            SzRelationsPage     expected,
                                            Class<?>            exceptionType)
        throws Exception
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], relationBound=[ " + relationBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        withConditionalSuppression(exceptionType != null, () -> {
            try {
                SzRelationsPage actual = this.getSummaryDisclosedRelations(
                    repoType,
                    connProvider,
                    dataSource,
                    vsDataSource,
                    matchKey,
                    principle,
                    relationBound,
                    boundType,
                    pageSize,
                    sampleSize);

                if (exceptionType != null) {
                    fail("Method unexpectedly succeeded.  " + testInfo);
                }

                this.validateRelationsPage(repoType,
                                           testInfo,
                                           relationBound,
                                           boundType,
                                           pageSize,
                                           sampleSize,
                                           expected,
                                           actual);

            } catch (Exception e) {
                validateException(testInfo, e, exceptionType);
            }
        });
    }

    /**
     * Gets the {@link SzRelationsPage} of disclosed relations for the specified cross-source parameters.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param dataSource The primary data source.
     * @param vsDataSource The versus data source.
     * @param matchKey The match key filter, or {@code null} for none.
     * @param principle The principle filter, or {@code null} for none.
     * @param relationBound The relation boundary value for pagination, or {@code null} for none.
     * @param boundType The {@link SzBoundType} for the boundary.
     * @param pageSize The maximum page size, or {@code null} for no limit.
     * @param sampleSize The sample size for random sampling, or {@code null} for no sampling.
     * @return The {@link SzRelationsPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzRelationsPage getSummaryDisclosedRelations(RepositoryType      repoType,
                                                           ConnectionProvider  connProvider,
                                                           String              dataSource,
                                                           String              vsDataSource,
                                                           String              matchKey,
                                                           String              principle,
                                                           String              relationBound,
                                                           SzBoundType         boundType,
                                                           Integer             pageSize,
                                                           Integer             sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return SummaryStatsReports.getSummaryDisclosedRelations(
                conn,
                dataSource,
                vsDataSource,
                matchKey,
                principle,
                relationBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

}
