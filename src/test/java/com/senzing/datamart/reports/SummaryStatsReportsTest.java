package com.senzing.datamart.reports;

import static com.senzing.datamart.model.SzMatchType.AMBIGUOUS_MATCH;
import static com.senzing.datamart.model.SzMatchType.DISCLOSED_RELATION;
import static com.senzing.datamart.model.SzMatchType.POSSIBLE_MATCH;
import static com.senzing.datamart.model.SzMatchType.POSSIBLE_RELATION;
import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.SzEntitiesPageParameters;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.handlers.CrossSourceKey;
import com.senzing.datamart.handlers.MatchPairKey;
import com.senzing.datamart.model.SzMatchType;
import com.senzing.datamart.model.SzRecord;
import com.senzing.datamart.model.SzRelatedEntity;
import com.senzing.datamart.model.SzResolvedEntity;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzCrossSourceMatchCounts;
import com.senzing.datamart.reports.model.SzCrossSourceRelationCounts;
import com.senzing.datamart.reports.model.SzCrossSourceSummary;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzMatchCounts;
import com.senzing.datamart.reports.model.SzRelationCounts;
import com.senzing.datamart.reports.model.SzSourceSummary;
import com.senzing.datamart.reports.model.SzSummaryStats;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.SQLUtilities;

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

    public List<Arguments> getSummaryStatsParameters() {
        List<Arguments> result = new LinkedList<>();
        
        this.summaryStatsMap.forEach((repoType, summaryStats) -> {
            ConnectionProvider connProvider = this.getConnectionProvider(repoType);

            Set<String> emptySet = Collections.emptySet();

            result.add(Arguments.of(repoType, connProvider, "*", "*", null, summaryStats));
            result.add(Arguments.of(repoType, connProvider, "*", "*", emptySet, summaryStats));

            // get the set of all loaded data sources
            Repository  repo            = DataMartTestExtension.getRepository(repoType);
            Set<String> loadedSources   = repo.getLoadedDataSources();

            result.add(Arguments.of(repoType, connProvider, "*", "*", loadedSources, summaryStats));

            // figure out which data sources are configured with no loaded records
            Set<String> dataSources = repo.getConfiguredDataSources();

            // determine the unused sources
            Set<String> unusedSources = new TreeSet<>(dataSources);
            unusedSources.removeAll(loadedSources);
            unusedSources = Collections.unmodifiableSet(unusedSources);

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

            result.add(Arguments.of(repoType, connProvider, "*", "*", dataSources, allStats));
            result.add(Arguments.of(repoType, connProvider, "*", "*", unusedSources, allStats));
            
            Map<String, Set<String>> matchKeyMap = repo.getRelatedMatchKeys();
            Map<String, Set<String>> principleMap = repo.getRelatedPrinciples();

            final Set<String> allUnusedSources = unusedSources;
            final int[] iteration = { 0 };
            // loop through the match key and principle options
            matchKeyMap.forEach((matchKey, principles) -> {
                if (((iteration[0]++) % 2) == 0) {
                    return;        
                }
                result.add(Arguments.of(
                    repoType, connProvider, matchKey, "*", null,
                    filter(allStats, loadedSources, null, matchKey, "*")));
                
                result.add(Arguments.of(
                    repoType, connProvider, matchKey, null, null,
                    filter(allStats, loadedSources, null, matchKey, null)));
                
                result.add(Arguments.of(
                    repoType, connProvider, matchKey, "*", allUnusedSources,
                    filter(allStats, loadedSources, allUnusedSources, matchKey, "*")));
                
                result.add(Arguments.of(
                    repoType, connProvider, matchKey, null, allUnusedSources,
                    filter(allStats, loadedSources, allUnusedSources, matchKey, null)));

                principles.forEach(principle -> {
                    if (((iteration[0]++) % 2) == 0) {
                        return;        
                    }
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, principle, null,
                        filter(allStats, loadedSources, null, matchKey, principle)));
                    
                    result.add(Arguments.of(
                        repoType, connProvider, matchKey, principle, allUnusedSources,
                        filter(allStats, loadedSources, allUnusedSources, matchKey, principle)));
                });
            });

            // loop through the principle keys
            principleMap.keySet().forEach(principle -> {
                if (((iteration[0]++) % 2) == 0) {
                    return;        
                }
                result.add(Arguments.of(
                    repoType, connProvider, "*", principle, null,
                    filter(allStats, loadedSources, null, "*", principle)));
                
                result.add(Arguments.of(
                    repoType, connProvider, null, principle, null,
                    filter(allStats, loadedSources, null, null, principle)));

                result.add(Arguments.of(
                    repoType, connProvider, "*", principle, allUnusedSources,
                    filter(allStats, loadedSources, allUnusedSources, "*", principle)));
                
                result.add(Arguments.of(
                    repoType, connProvider, null, principle, allUnusedSources,
                    filter(allStats, loadedSources, allUnusedSources, null, principle)));
            });


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
                        repoType, connProvider, "*", "*", extraSources, 
                        filter(allStats, loadedSources, extraSources, "*", "*")));

                    result.add(Arguments.of(
                        repoType, connProvider, null, "*", extraSources, 
                        filter(allStats, loadedSources, extraSources, null, "*")));
                    
                    result.add(Arguments.of(
                        repoType, connProvider, "*", null, extraSources, 
                        filter(allStats, loadedSources, extraSources, "*", null)));

                    result.add(Arguments.of(
                        repoType, connProvider, null, null, extraSources, 
                        filter(allStats, loadedSources, extraSources, null, null)));

                    // loop through the match key and principle options
                    iteration[0] = 0;
                    matchKeyMap.forEach((matchKey, principles) -> {
                        if (((iteration[0]++) % 2) == 1) {
                            return;        
                        }
                        result.add(Arguments.of(
                            repoType, connProvider, matchKey, "*", extraSources,
                            filter(allStats, loadedSources, extraSources, matchKey, "*")));
                        
                        result.add(Arguments.of(
                            repoType, connProvider, matchKey, null, extraSources,
                            filter(allStats, loadedSources, extraSources, matchKey, null)));

                        principles.forEach(principle -> {
                            if (((iteration[0]++) % 2) == 1) {
                                return;        
                            }
                            result.add(Arguments.of(
                                repoType, connProvider, matchKey, principle, extraSources,
                                filter(allStats, loadedSources, extraSources, matchKey, principle)));
                        });
                    });

                    // loop through the principle keys
                    principleMap.keySet().forEach(principle -> {
                        if (((iteration[0]++) % 2) == 1) {
                            return;        
                        }
                       result.add(Arguments.of(
                            repoType, connProvider, "*", principle, extraSources,
                            filter(allStats, loadedSources, extraSources, "*", principle)));
                        
                        result.add(Arguments.of(
                            repoType, connProvider, null, principle, extraSources,
                            filter(allStats, loadedSources, extraSources, null, principle)));
                    });
                }
            }
        });

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<Arguments> getSourceSummaryParameters() {
        List<Arguments> result = new LinkedList<>();

        List<Arguments> summaryParams = this.getSummaryStatsParameters();
        for (Arguments origParams : summaryParams) {
            Object[]            paramArray      = origParams.get();
            RepositoryType      repoType        = (RepositoryType) paramArray[0];
            ConnectionProvider  connProvider    = (ConnectionProvider) paramArray[1];
            String              matchKey        = (String) paramArray[2];
            String              principle       = (String) paramArray[3];
            Set<String>         extraSources    = (Set<String>) paramArray[4];
            SzSummaryStats      summaryStats    = (SzSummaryStats) paramArray[5];

            for (SzSourceSummary summary : summaryStats.getSourceSummaries()) {
                String dataSource = summary.getDataSource();
                result.add(Arguments.of(
                    repoType, connProvider, dataSource, matchKey, principle, extraSources, summary));
            }
        }
        return result;
    }

    public List<Arguments> getCrossSummaryParameters() {
        List<Arguments> result = new LinkedList<>();

        List<Arguments> summaryParams = this.getSourceSummaryParameters();
        Set<List<?>>  accountedFor  = new HashSet<>();
        
        for (Arguments origParams : summaryParams) {
            Object[]            paramArray      = origParams.get();
            RepositoryType      repoType        = (RepositoryType) paramArray[0];
            ConnectionProvider  connProvider    = (ConnectionProvider) paramArray[1];
            String              dataSource      = (String) paramArray[2];
            String              matchKey        = (String) paramArray[3];
            String              principle       = (String) paramArray[4];
            SzSourceSummary     sourceSummary   = (SzSourceSummary) paramArray[6];

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
                    repoType, connProvider, dataSource, vsDataSource, matchKey, principle, cross));
            }
        }
        return result;
    }

    @ParameterizedTest()
    @MethodSource("getSummaryStatsParameters")
    public void testSummaryStats(RepositoryType      repoType,
                                 ConnectionProvider  connProvider,
                                 String              matchKey,
                                 String              principle,
                                 Set<String>         dataSources,
                                 SzSummaryStats      expected)                                
    {
        String testInfo = "repoType=[ " + repoType + " ], matchKey=[ " + matchKey
            + " ], principle=[ " + principle + " ], dataSources=[ " + dataSources
            + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzSummaryStats actual 
                = SummaryStatsReports.getSummaryStatistics(conn, 
                                                           matchKey,
                                                           principle,
                                                           dataSources,
                                                           null);

            validateReport(expected, actual, testInfo);

        } catch (Exception e) {
            fail("Failed test with an unexpected exception: repoType=[ "
                 + repoType + " ]", e);

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
                                  Set<String>           dataSources,
                                  SzSourceSummary       expected)                                
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ " 
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], dataSources=[ " + dataSources + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzSourceSummary actual 
                = SummaryStatsReports.getSourceSummary(conn, 
                                                       dataSource,
                                                       matchKey,
                                                       principle,
                                                       dataSources,
                                                       null);

            validateReport(expected, actual, testInfo);

        } catch (Exception e) {
            fail("Failed test with an unexpected exception: repoType=[ "
                 + repoType + " ]", e);

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
                                 SzCrossSourceSummary  expected)                                
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ " 
            + dataSource + " ], vsDataSource=[ " + vsDataSource 
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzCrossSourceSummary actual 
                = SummaryStatsReports.getCrossSourceSummary(conn, 
                                                            dataSource,
                                                            vsDataSource,
                                                            matchKey,
                                                            principle,
                                                            null);

            validateReport(expected, actual, testInfo);

            SzCrossSourceMatchCounts matchCounts
                = SummaryStatsReports.getCrossSourceMatchSummary(
                    conn, dataSource, vsDataSource, matchKey, principle, null);

            validateReport(expected.getMatches(), 
                           matchCounts.getCounts(),
                           "CrossSourceMatchSummary",
                           testInfo);

            SzCrossSourceRelationCounts relationCounts
                = SummaryStatsReports.getCrossSourceAmbiguousMatchSummary(
                    conn, dataSource, vsDataSource, matchKey, principle, null);

            validateReport(expected.getAmbiguousMatches(), 
                           relationCounts.getCounts(), 
                           "CrossSourceAmbiguousMatchSummary",
                           testInfo);
            
            relationCounts = SummaryStatsReports.getCrossSourcePossibleMatchSummary(
                    conn, dataSource, vsDataSource, matchKey, principle, null);

            validateReport(expected.getPossibleMatches(), 
                           relationCounts.getCounts(),
                           "CrossSourcePossibleMatchSummary",
                           testInfo);
            
            relationCounts = SummaryStatsReports.getCrossSourcePossibleRelationSummary(
                    conn, dataSource, vsDataSource, matchKey, principle, null);

            validateReport(expected.getPossibleRelations(), 
                           relationCounts.getCounts(),
                           "CrossSourcePossibleRelationSummary",
                           testInfo);

            relationCounts = SummaryStatsReports.getCrossSourceDisclosedRelationSummary(
                    conn, dataSource, vsDataSource, matchKey, principle, null);

            validateReport(expected.getDisclosedRelations(), 
                           relationCounts.getCounts(),
                           "CrossSourceDisclosedRelationSummary",
                           testInfo);

        } catch (Exception e) {
            fail("Failed test with an unexpected exception: repoType=[ "
                 + repoType + " ]", e);

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
        public boolean test(SzRelatedEntity  entity,
                            String           dataSource, 
                            String           vsDataSource,
                            String           matchKey,
                            String           principle);
    }

    public List<Arguments> getSummaryEntitiesParameters(EntityQualifier qualifier) {
        List<Arguments> result = new LinkedList<>();

        for (RepositoryType repoType : RepositoryType.values()) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

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
                        if (skipIndex++ % 2 != 0) {
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
                    // check if the record has the expected data source
                    if (!record.getDataSource().equals(vsDataSource)) {
                        continue;
                    }
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

                    // check if the related entity has the match key and principle
                    if ((matchKey == null || "*".equals(matchKey) 
                        || matchKey.equals(related.getMatchKey()))
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

        for (RepositoryType repoType : RepositoryType.values()) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

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
                            if (skipIndex++ % 2 != 0) {
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

                    // check if the related entity has the match key and principle
                    if ((matchKey == null || "*".equals(matchKey) 
                        || matchKey.equals(related.getMatchKey()))
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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getSummaryMatchEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);

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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getSummaryAmbiguousMatchEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);

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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getSummaryPossibleMatchEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);

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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getSummaryPossibleRelationEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);

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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getSummaryDisclosedRelationEntityIds(
                conn,
                dataSource,
                matchKey,
                principle,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);

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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource 
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getCrossMatchEntityIds(
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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource 
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getCrossAmbiguousMatchEntityIds(
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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource 
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getCrossPossibleMatchEntityIds(
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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource 
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getCrossPossibleRelationEntityIds(
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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

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
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], vsDataSource=[ " + vsDataSource 
            + " ], matchKey=[ " + matchKey + " ], principle=[ "
            + principle + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = SummaryStatsReports.getCrossDisclosedRelationEntityIds(
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
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

        } finally {
            SQLUtilities.close(conn);
        }
    }

}
