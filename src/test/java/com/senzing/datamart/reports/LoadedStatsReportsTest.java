package com.senzing.datamart.reports;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzLoadedStats;
import com.senzing.datamart.reports.model.SzSourceLoadedStats;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.SQLUtilities;

@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class LoadedStatsReportsTest extends AbstractReportsTest {
    
    private Map<RepositoryType, SzLoadedStats> loadedStatsMap = null;

    @Override
    @BeforeAll
    public void setup() throws Exception {
        super.setup();
        this.loadedStatsMap = this.getLoadedStats();
    }

    public List<Arguments> getLoadedStatsParameters() {
        List<Arguments> result = new ArrayList<>(this.loadedStatsMap.size());
        
        this.loadedStatsMap.forEach((repoType, loadedStats) -> {
            ConnectionProvider connProvider = this.getConnectionProvider(repoType);

            Set<String> emptySet = Collections.emptySet();

            result.add(Arguments.of(repoType, connProvider, null, loadedStats));
            result.add(Arguments.of(repoType, connProvider, emptySet, loadedStats));

            // get the set of all loaded data sources
            Repository  repo            = DataMartTestExtension.getRepository(repoType);
            Set<String> loadedSources   = repo.getLoadedDataSources();

            result.add(Arguments.of(repoType, connProvider, loadedSources, loadedStats));

            // figure out which data sources are configured with no loaded records
            Set<String> dataSources     = repo.getConfiguredDataSources();
            Set<String> unusedSources   = new TreeSet<>(dataSources);
            unusedSources.removeAll(loadedSources);
            unusedSources = Collections.unmodifiableSet(unusedSources);

            Map<String, SzSourceLoadedStats> unusedStatsMap = new TreeMap<>();

            // create source stats for each unused data source
            for (String unusedSource : unusedSources) {
                SzSourceLoadedStats sourceStats = new SzSourceLoadedStats(unusedSource);
                sourceStats.setEntityCount(0);
                sourceStats.setRecordCount(0);
                sourceStats.setUnmatchedRecordCount(0);
                unusedStatsMap.put(unusedSource, sourceStats);
            }

            SzLoadedStats allStats = clone(loadedStats);
            for (SzSourceLoadedStats stats : unusedStatsMap.values()) {
                allStats.addDataSourceCount(stats);
            }

            result.add(Arguments.of(repoType, connProvider, dataSources, allStats));
            result.add(Arguments.of(repoType, connProvider, unusedSources, allStats));
            
            // check if we have more than one unused data source
            if (unusedSources.size() > 1) {
                // make smaller sets of unused sources of various sizes
                for (int count = 1; count < unusedSources.size(); count++) {
                    // setup the sources parameter and expected stats parameter
                    Iterator<String>    iter    = unusedSources.iterator();
                    Set<String>         sources = new TreeSet<>();
                    SzLoadedStats       stats   = clone(loadedStats);
                    
                    // we need the loaded stats always in the expected result
                    for (SzSourceLoadedStats sourceStats : stats.getDataSourceCounts()) {
                        stats.addDataSourceCount(sourceStats);
                    }

                    // now create a set of unused sources and add the zero stats to the
                    // expected result
                    for (int index = 0; index < count && iter.hasNext(); index++) {
                        String source = iter.next();
                        SzSourceLoadedStats sourceStats = unusedStatsMap.get(source);
                        stats.addDataSourceCount(sourceStats);
                        sources.add(source);
                    }

                    // make the source set unmodifiable
                    sources = Collections.unmodifiableSet(sources);

                    // add the test parameters to the list
                    result.add(Arguments.of(repoType, connProvider, sources, stats));            
                }
            }
        });

        return result;
    }

    @ParameterizedTest()
    @MethodSource("getLoadedStatsParameters")
    public void testLoadedStats(RepositoryType      repoType,
                                ConnectionProvider  connProvider,
                                Set<String>         dataSources,
                                SzLoadedStats       expected)                                
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSources=[ " 
            + dataSources + " ]";

        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzLoadedStats actual 
                = LoadedStatsReports.getLoadedStatistics(conn, dataSources, null);

            validateReport(expected, actual, testInfo);
            
        } catch (Exception e) {
            fail("Failed test with an unexpected exception: repoType=[ "
                 + repoType + " ]", e);

        } finally {
            SQLUtilities.close(conn);
        }
    }

    public List<Arguments> getSourceStatParameters() {
        List<Arguments> result = new LinkedList<>();
        
        this.loadedStatsMap.forEach((repoType, loadedStats) -> {
            ConnectionProvider connProvider = this.getConnectionProvider(repoType);

            Repository repo = DataMartTestExtension.getRepository(repoType);

            Set<String> dataSources = repo.getConfiguredDataSources();
            Set<String> loadedSources = repo.getLoadedDataSources();

            Map<String, SzSourceLoadedStats> sourceStatsMap = new TreeMap<>();

            for (SzSourceLoadedStats sourceStats : loadedStats.getDataSourceCounts()) {
                sourceStatsMap.put(sourceStats.getDataSource(), sourceStats);
            }

            for (String source : dataSources) {
                // skip loaded sources
                if (loadedSources.contains(source)) {
                    continue;
                }

                // create source stats for an unused source
                SzSourceLoadedStats sourceStats = new SzSourceLoadedStats(source);
                sourceStats.setEntityCount(0);
                sourceStats.setRecordCount(0);
                sourceStats.setUnmatchedRecordCount(0);

                // record the stats
                sourceStatsMap.put(source, sourceStats);
            }

            // loop through the data sources
            dataSources.forEach((source) -> {
                result.add(Arguments.of(repoType, 
                                        connProvider,
                                        source,
                                        sourceStatsMap.get(source),
                                        null));
            });

            // create a test for a null data source
            result.add(Arguments.of(repoType,
                                    connProvider,
                                    null,
                                    null,
                                    NullPointerException.class));
        });

        return result;
    }

    @ParameterizedTest()
    @MethodSource("getSourceStatParameters")
    public void testEntitySizeCount(RepositoryType      repoType,
                                    ConnectionProvider  connProvider,
                                    String              dataSource,
                                    SzSourceLoadedStats expected,
                                    Class<?>            exceptionType)
    {
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ " 
            + dataSource + " ], expectedException=[ " + exceptionType + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzSourceLoadedStats actual 
                = LoadedStatsReports.getSourceLoadedStatistics(conn, dataSource, null);
                
            if (exceptionType != null) {
                fail("Method unexpectedly succeeded. " + testInfo);
            }

            validateReport(expected, actual, testInfo);

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

    public List<Arguments> getEntitiesForSourceParameters() {
        List<Arguments> result = new LinkedList<>();

        this.loadedStatsMap.forEach((repoType, loadedStats) -> {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            Set<String> dataSources = repo.getConfiguredDataSources();

            for (String source : dataSources) {
                List<SzEntitiesPageParameters> params 
                    = this.generateEntitiesPageParameters(
                        repoType, (e) -> e.getSourceSummary().containsKey(source));
                        
                for (SzEntitiesPageParameters p : params) {
                    result.add(Arguments.of(repoType,
                                            this.getConnectionProvider(repoType),
                                            source,
                                            p.getEntityIdBound(),
                                            p.getBoundType(),
                                            p.getPageSize(),
                                            p.getSampleSize(),
                                            p.getExpectedPage(),
                                            null));
                }
            }
        });
        return result;
    }

    @ParameterizedTest
    @MethodSource("getEntitiesForSourceParameters")
    public void testEntitiesForSource(RepositoryType        repoType,
                                      ConnectionProvider    connProvider,
                                      String                dataSource,
                                      String                entityIdBound,
                                      SzBoundType           boundType,
                                      Integer               pageSize,
                                      Integer               sampleSize,
                                      SzEntitiesPage        expected,
                                      Class<?>              exceptionType)
    {                              
        String testInfo = "repoType=[ " + repoType + " ], dataSource=[ "
            + dataSource + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = LoadedStatsReports.getEntityIdsForDataSource(
                conn,
                dataSource,
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
