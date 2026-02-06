package com.senzing.datamart.reports;

import static com.senzing.datamart.reports.model.SzBoundType.EXCLUSIVE_LOWER;
import static com.senzing.datamart.reports.model.SzBoundType.EXCLUSIVE_UPPER;
import static com.senzing.datamart.reports.model.SzBoundType.INCLUSIVE_LOWER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.handlers.CrossMatchKey;
import com.senzing.datamart.handlers.CrossRelationKey;
import com.senzing.datamart.handlers.CrossSourceKey;
import com.senzing.datamart.handlers.MatchPairKey;
import com.senzing.datamart.handlers.SourceRelationKey;
import com.senzing.datamart.model.SzMatchType;
import com.senzing.datamart.model.SzRecord;
import com.senzing.datamart.model.SzRecordKey;
import com.senzing.datamart.model.SzRelatedEntity;
import com.senzing.datamart.model.SzResolvedEntity;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzCrossSourceSummary;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntityRelationsBreakdown;
import com.senzing.datamart.reports.model.SzEntityRelationsCount;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.datamart.reports.model.SzLoadedStats;
import com.senzing.datamart.reports.model.SzMatchCounts;
import com.senzing.datamart.reports.model.SzRelationCounts;
import com.senzing.datamart.reports.model.SzRelationKey;
import com.senzing.datamart.reports.model.SzRelationType;
import com.senzing.datamart.reports.model.SzRelationsPage;
import com.senzing.datamart.reports.model.SzReportEntity;
import com.senzing.datamart.reports.model.SzReportRecord;
import com.senzing.datamart.reports.model.SzReportRelation;
import com.senzing.datamart.reports.model.SzSourceLoadedStats;
import com.senzing.datamart.reports.model.SzSourceSummary;
import com.senzing.datamart.reports.model.SzSummaryStats;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.Connector;
import com.senzing.sql.PoolConnectionProvider;
import uk.org.webcompere.systemstubs.stream.SystemErrAndOut;

import static com.senzing.util.JsonUtilities.*;

/**
 * Provides a base class for reports tests.
 */
@Execution(ExecutionMode.SAME_THREAD)
public abstract class AbstractReportsTest {
    /**
     * The {@link ObjectMapper} to use for JSON serialization of reports.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * A {@link Map} of {@link RepositoryType} keys to
     * {@link ConnectionPool} instances for connecting to that
     * data mart database.
     */
    private Map<RepositoryType, ConnectionPool> connPoolMap 
        = Collections.synchronizedMap(new HashMap<>());

    /**
     * A {@link Map} of {@link RepositoryType} keys to
     * {@link ConnectionProvider} instances for connecting to that
     * data mart database.
     * 
     * The {@link ConnectionProvider} instances are backed by the
     * respective {@link ConnectionPool} from {@link #connPoolMap}.
     */
    private Map<RepositoryType, ConnectionProvider> connProviderMap 
        = Collections.synchronizedMap(new HashMap<>());

    @BeforeAll
    public void setup() throws Exception {
        RepositoryType[] repoTypes = RepositoryType.values();
        for (RepositoryType repoType : repoTypes) {
            Repository repo = DataMartTestExtension.getRepository(repoType);
            Connector connector = repo.getDataMartConnector();
            ConnectionPool connPool = new ConnectionPool(connector, 1, 4);
            connPoolMap.put(repoType, connPool);
            ConnectionProvider connProv = new PoolConnectionProvider(connPool);
            connProviderMap.put(repoType, connProv);
        }
    }

    @AfterAll
    public void teardown() {
        for (ConnectionPool connPool : connPoolMap.values()) {
            connPool.shutdown();
        }
        connPoolMap.clear();
        connProviderMap.clear();
    }

    /**
     * Gets the {@link ConnectionPool} associated with the specified
     * {@link RepositoryType} for connecting to the test data mart of
     * that type.
     * 
     * @param repoType The {@link RepositoryType} for which the {@link
     *                 ConnectionPool} is being requested.
     * 
     * @return The {@link ConnectionPool} for the specified 
     *         {@link RepositoryType}.
     * 
     * @throws NullPointerException If the specified parameter is 
     *                              <code>null</code>.
     */
    protected ConnectionPool getConnectionPool(RepositoryType repoType) {
        Objects.requireNonNull(repoType, "Repository type cannot be null");
        return this.connPoolMap.get(repoType);
    }

    /**
     * Gets the {@link ConnectionProvider} associated with the specified
     * {@link RepositoryType} for connecting to the test data mart of
     * that type.
     * 
     * @param repoType The {@link RepositoryType} for which the {@link
     *                 ConnectionProvider} is being requested.
     * 
     * @return The {@link ConnectionProvider} for the specified 
     *         {@link RepositoryType}.
     * 
     * @throws NullPointerException If the specified parameter is 
     *                              <code>null</code>.
     */
    protected ConnectionProvider getConnectionProvider(RepositoryType repoType) {
        Objects.requireNonNull(repoType, "Repository type cannot be null");
        return this.connProviderMap.get(repoType);
    }

    /**
     * Functional interface for code that can throw exceptions.
     * Used with {@link #withConditionalSuppression(boolean, ThrowingRunnable)}.
     */
    @FunctionalInterface
    protected interface ThrowingRunnable {
        /**
         * Runs the code that may throw an exception.
         *
         * @throws Exception If an error occurs.
         */
        void run() throws Exception;
    }

    /**
     * Executes the specified runnable, conditionally suppressing System.err
     * and System.out if the condition is true. This is useful for parameterized
     * tests where some test cases expect exceptions (which produce console output)
     * while others expect normal execution.
     *
     * @param suppress Whether to suppress console output.
     * @param runnable The code to execute.
     * @throws Exception If an error occurs during execution.
     */
    protected void withConditionalSuppression(boolean suppress,
                                              ThrowingRunnable runnable)
        throws Exception
    {
        if (suppress) {
            new SystemErrAndOut().execute(() -> {
                runnable.run();
                return null;
            });
        } else {
            runnable.run();
        }
    }

    /**
     * Creates a {@link Map} of {@link RepositoryType} keys to 
     * {@link SzEntitySizeBreakdown} values describing the expected
     * entity size breakdown reports for the respective repository.
     * 
     * @return A {@link Map} of {@link RepositoryType} keys to 
     *         {@link SzEntitySizeBreakdown} values describing the
     *         expected entity size breakdown reports for the 
     *         respective repository.
     */
    public Map<RepositoryType, SzEntitySizeBreakdown> getEntitySizeBreakdowns() 
    {
        RepositoryType[] repoTypes = RepositoryType.values();

        Map<RepositoryType, SzEntitySizeBreakdown> result 
            = new TreeMap<>();

        for (RepositoryType repoType : repoTypes) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            Map<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

            SortedMap<Integer, SzEntitySizeCount> sizeCounts = new TreeMap<>(
                (c1, c2) -> c2.intValue() - c1.intValue());
        
            // loop through the entities and size them up
            for (SzResolvedEntity entity : entities.values()) {
                // get the entity size
                int entitySize = entity.getRecords().size();

                // find the SzEntitySizeCount for that size
                SzEntitySizeCount sizeCount = sizeCounts.get(entitySize);

                // if none, create and cache the SzEntitySizeCount instance
                if (sizeCount == null) {
                    sizeCount = new SzEntitySizeCount();
                    sizeCount.setEntitySize(entitySize);
                    sizeCounts.put(entitySize, sizeCount);
                }

                // increment the entity count
                sizeCount.setEntityCount(sizeCount.getEntityCount() + 1);
            }

            List<SzEntitySizeCount> countList = new ArrayList<>(sizeCounts.values());
            SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();
            breakdown.setEntitySizeCounts(countList);

            result.put(repoType, breakdown);
        }

        return result;
    }

    /**
     * Creates a {@link Map} of {@link RepositoryType} keys to 
     * {@link SzEntityRelationsBreakdown} values describing the expected
     * entity relations breakdown reports for the respective repository.
     * 
     * @return A {@link Map} of {@link RepositoryType} keys to 
     *         {@link SzEntityRelationsBreakdown} values describing the
     *         expected entity size breakdown reports for the 
     *         respective repository.
     */
    public Map<RepositoryType, SzEntityRelationsBreakdown> getEntityRelationsBreakdowns() 
    {
        RepositoryType[] repoTypes = RepositoryType.values();

        Map<RepositoryType, SzEntityRelationsBreakdown> result 
            = new TreeMap<>();

        for (RepositoryType repoType : repoTypes) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            Map<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

            SortedMap<Integer, SzEntityRelationsCount> relCounts = new TreeMap<>(
                (c1, c2) -> c2.intValue() - c1.intValue());
        
            // loop through the entities and count relations
            for (SzResolvedEntity entity : entities.values()) {
                // get the entity relation count
                int count = entity.getRelatedEntities().size();

                // find the SzEntitySizeCount for that size
                SzEntityRelationsCount relCount = relCounts.get(count);

                // if none, create and cache the SzEntitySizeCount instance
                if (relCount == null) {
                    relCount = new SzEntityRelationsCount();
                    relCount.setRelationsCount(count);
                    relCounts.put(count, relCount);
                }

                // increment the entity count
                relCount.setEntityCount(relCount.getEntityCount() + 1);
            }

            List<SzEntityRelationsCount> countList = new ArrayList<>(relCounts.values());
            SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();
            breakdown.setEntityRelationsCounts(countList);

            result.put(repoType, breakdown);
        }

        return result;
    }

    /**
     * Clones the specified {@link SzLoadedStates} parameter into a new instance.
     * 
     * @param loadedStats The {@link SzLoadedStats} to clone.
     * 
     * @return The new instance of of {@link SzLoadedStats}, identical to the
     *         specified {@link SzLoadedStats} instance.
     */
    public static SzLoadedStats clone(SzLoadedStats loadedStats) {
        SzLoadedStats result = new SzLoadedStats();
        result.setTotalEntityCount(loadedStats.getTotalEntityCount());
        result.setTotalRecordCount(loadedStats.getTotalRecordCount());
        result.setTotalUnmatchedRecordCount(loadedStats.getTotalUnmatchedRecordCount());
        
        for (SzSourceLoadedStats sourceStats : loadedStats.getDataSourceCounts()) {
            result.addDataSourceCount(sourceStats);
        }
        return result;
    }

    /**
     * Creates a filtered clone the specified {@link SzSummaryStats} based
     * on the specified parameters.
     * 
     * @param summaryStats The original instance from which to create the filtered clone.
     * 
     * @param loadedSources The {@link Set} of data sources with loaded records.
     * 
     * @param extraSources The {@link Set} of data sources that do not have loaded
     *                     records, but should be included.
     * 
     * @param matchKey The match key to filter on which can be <code>null</code> to
     *                 indicate only the counts not associated with a specific match 
     *                 key should be included, or <code>"*"</code> to indicate no 
     *                 filtering, or a specific match key.
     * 
     * @param principle The principle to filter on which can be <code>null</code> to
     *                  indicate only the counts not associated with a specific 
     *                  principle should be included, or <code>"*"</code> to indicate
     *                  no filtering, or a specific principle.
     * 
     * @return The new instance of of {@link SzSummaryStats}, which is a filtered
     *         clone of the specified instance.
     */
    public static SzSummaryStats filter(SzSummaryStats  summaryStats,
                                        Set<String>     loadedSources,
                                        Set<String>     extraSources,
                                        String          matchKey, 
                                        String          principle) 
    {
        SzSummaryStats result = new SzSummaryStats();
        
        for (SzSourceSummary summary : summaryStats.getSourceSummaries()) {
            if (loadedSources.contains(summary.getDataSource())
                || (extraSources != null && extraSources.contains(summary.getDataSource())))
            {
                result.addSourceSummary(
                    filter(summary, loadedSources, extraSources, matchKey, principle));
            }
        }
        return result;
    }

    /**
     * Creates a filtered clone the specified {@link SzSourceSummary} based
     * on the specified parameters.
     * 
     * @param sourceSummary The original instance from which to create the filtered clone.
     * 
     * @param loadedSources The {@link Set} of data sources with loaded records.
     * 
     * @param extraSources The {@link Set} of data sources that do not have loaded
     *                     records, but should be included.
     * 
     * @param matchKey The match key to filter on which can be <code>null</code> to
     *                 indicate only the counts not associated with a specific match 
     *                 key should be included, or <code>"*"</code> to indicate no 
     *                 filtering, or a specific match key.
     * 
     * @param principle The principle to filter on which can be <code>null</code> to
     *                  indicate only the counts not associated with a specific 
     *                  principle should be included, or <code>"*"</code> to indicate
     *                  no filtering, or a specific principle.
     * 
     * @return The new instance of of {@link SzSourceSummary}, which is a filtered
     *         clone of the specified instance.
     */
    public static SzSourceSummary filter(SzSourceSummary sourceSummary,
                                         Set<String>     loadedSources,
                                         Set<String>     extraSources,
                                         String          matchKey, 
                                         String          principle) 
    {
        SzSourceSummary result = new SzSourceSummary(sourceSummary.getDataSource());
        result.setEntityCount(sourceSummary.getEntityCount());
        result.setRecordCount(sourceSummary.getRecordCount());
        result.setUnmatchedRecordCount(sourceSummary.getUnmatchedRecordCount());
        
        for (SzCrossSourceSummary cross : sourceSummary.getCrossSourceSummaries()) {
            if (loadedSources.contains(cross.getVersusDataSource())
                || (extraSources != null && extraSources.contains(cross.getVersusDataSource())))
            {
                result.addCrossSourceSummary(
                    filter(cross, matchKey, principle));
            }
        }
        return result;
    }

    /**
     * Creates a filtered clone the specified {@link SzCrossSourceSummary} based
     * on the specified parameters.
     * 
     * @param crossSummary The original instance from which to create the filtered clone.
     * 
     * @param matchKey The match key to filter on which can be <code>null</code> to
     *                 indicate only the counts not associated with a specific match 
     *                 key should be included, or <code>"*"</code> to indicate no 
     *                 filtering, or a specific match key.
     * 
     * @param principle The principle to filter on which can be <code>null</code> to
     *                  indicate only the counts not associated with a specific 
     *                  principle should be included, or <code>"*"</code> to indicate
     *                  no filtering, or a specific principle.
     * 
     * @return The new instance of of {@link SzCrossSourceSummary}, which is a 
     *         filtered clone of the specified instance.
     */
    public static SzCrossSourceSummary filter(SzCrossSourceSummary  crossSummary, 
                                              String                matchKey,
                                              String                principle) 
    {
        SzCrossSourceSummary result = new SzCrossSourceSummary(
            crossSummary.getDataSource(), crossSummary.getVersusDataSource());

        for (SzMatchCounts counts : crossSummary.getMatches()) {
            if (("*".equals(matchKey) || Objects.equals(matchKey, counts.getMatchKey()))
                && ("*".equals(principle) || Objects.equals(principle, counts.getPrinciple()))) 
            {
                result.addMatches(counts);
            }
        }
        for (SzRelationCounts counts : crossSummary.getAmbiguousMatches()) {
            if (("*".equals(matchKey) || Objects.equals(matchKey, counts.getMatchKey()))
                && ("*".equals(principle) || Objects.equals(principle, counts.getPrinciple()))) 
            {
                result.addAmbiguousMatches(counts);
            }
        }
        for (SzRelationCounts counts : crossSummary.getPossibleMatches()) {
            if (("*".equals(matchKey) || Objects.equals(matchKey, counts.getMatchKey()))
                && ("*".equals(principle) || Objects.equals(principle, counts.getPrinciple()))) 
            {
                result.addPossibleMatches(counts);
            }
        }
        for (SzRelationCounts counts : crossSummary.getPossibleRelations()) {
            if (("*".equals(matchKey) || Objects.equals(matchKey, counts.getMatchKey()))
                && ("*".equals(principle) || Objects.equals(principle, counts.getPrinciple()))) 
            {
                result.addPossibleRelations(counts);
            }
        }
        for (SzRelationCounts counts : crossSummary.getDisclosedRelations()) {
            if (("*".equals(matchKey) || Objects.equals(matchKey, counts.getMatchKey()))
                && ("*".equals(principle) || Objects.equals(principle, counts.getPrinciple()))) 
            {
                result.addDisclosedRelations(counts);
            }
        }

        SzMatchCounts zeroMatches = new SzMatchCounts(
            "*".equals(matchKey) ? null : matchKey, 
            "*".equals(principle) ? null : principle);
        
        SzRelationCounts zeroRelations = new SzRelationCounts(
            "*".equals(matchKey) ? null : matchKey, 
            "*".equals(principle) ? null : principle);
        
        if (result.getMatches().size() == 0) {
            result.addMatches(zeroMatches);
        }
        if (result.getAmbiguousMatches().size() == 0) {
            result.addAmbiguousMatches(zeroRelations);
        }
        if (result.getPossibleMatches().size() == 0) {
            result.addPossibleMatches(zeroRelations);
        }
        if (result.getPossibleRelations().size() == 0) {
            result.addPossibleRelations(zeroRelations);
        }
        if (result.getDisclosedRelations().size() == 0) {
            result.addDisclosedRelations(zeroRelations);
        }

        return result;
    }

    /**
     * Creates a {@link Map} of {@link RepositoryType} keys to 
     * {@link SzLoadedStats} values describing the expected
     * loaded stats reports for the respective repository.
     * 
     * @return A {@link Map} of {@link RepositoryType} keys to 
     *         {@link SzLoadedStats} values describing the expected
     *         loaded stats reports for the respective repository.
     */
    public Map<RepositoryType, SzLoadedStats> getLoadedStats() {
        Map<RepositoryType, SzLoadedStats> result = new TreeMap<>();

        RepositoryType[] repoTypes = RepositoryType.values();

        for (RepositoryType repoType : repoTypes) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            Map<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

            SzLoadedStats loadedStats = new SzLoadedStats();
            SortedMap<String, SzSourceLoadedStats> sourceStatMap = new TreeMap<>();

            // loop through the entities and tally statistics
            for (SzResolvedEntity entity : entities.values()) {
                // get the records
                Map<SzRecordKey, SzRecord> records = entity.getRecords();

                loadedStats.setTotalEntityCount(loadedStats.getTotalEntityCount() + 1);
                if (records.size() == 1) {
                    loadedStats.setTotalUnmatchedRecordCount(
                        loadedStats.getTotalUnmatchedRecordCount() + 1);
                }
                loadedStats.setTotalRecordCount(
                    loadedStats.getTotalRecordCount() + records.size());

                // get the source summary
                Map<String, Integer> sourceSummary = entity.getSourceSummary();

                // iterate over the data sources
                sourceSummary.forEach((dataSource, recordCount) -> {
                    // find the source counts for the data source
                    SzSourceLoadedStats sourceStats = sourceStatMap.get(dataSource);

                    // if none, create and cache the SzEntitySizeCount instance
                    if (sourceStats == null) {
                        sourceStats = new SzSourceLoadedStats(dataSource);
                        sourceStatMap.put(dataSource, sourceStats);
                    }

                    // increment the entity count by 1
                    sourceStats.setEntityCount(sourceStats.getEntityCount() + 1);

                    // increment the record count by the summary record count
                    sourceStats.setRecordCount(sourceStats.getRecordCount() + recordCount);

                    // if this is a singleton entity then increment the singleton count
                    if (records.size() == 1) {
                        sourceStats.setUnmatchedRecordCount(
                            sourceStats.getUnmatchedRecordCount() + 1);
                    }
                });
                
                // set the source loaded stats
                loadedStats.setDataSourceCounts(sourceStatMap.values());

                // store in the result
                result.put(repoType, loadedStats);
            }
        }

        // return the result
        return result;
    }

    /**
     * Generates various entity page parameter combinations for the
     * specified {@link RepositoryType}, using the specified 
     * {@link Predicate} to qualify entities from the repository.
     * 
     * @param repoType The respective {@link RepositoryType}.
     * 
     * @param predicate The {@link Predicate} for qualifying loaded entities
     *                  from the repository.
     * 
     * @return The {@link List} of {@link SzEntitiesPageParameters} to aid
     *         in testing the entities page retrieval.
     */
    public List<SzEntitiesPageParameters> generateEntitiesPageParameters(
        RepositoryType              repoType,
        Predicate<SzResolvedEntity> predicate)
    {
        List<SzEntitiesPageParameters> result = new LinkedList<>();

        Repository repo = DataMartTestExtension.getRepository(repoType);

        SortedMap<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

        SortedMap<Long, SzResolvedEntity> filtered = new TreeMap<>(entities);

        long minEntityId = -1L;
        long maxEntityId = -1L;
        // loop over the entities
        for (Map.Entry<Long, SzResolvedEntity> entry : entities.entrySet()) {
            SzResolvedEntity    entity      = entry.getValue();
            long                entityId    = entry.getKey();
            if (predicate.test(entity)) {
                // track the minimum and maximum entity ID
                if (minEntityId < 0L || entityId < minEntityId) 
                {
                    minEntityId = entityId;
                }
                if (maxEntityId < 0L || entityId > maxEntityId) 
                {
                    maxEntityId = entityId;
                }
            } else {
                filtered.remove(entityId);
            }
        }

        // try using entity ID bound as "0" with null bound type and
        // null page size and null sample size (to use default page size)
        SzEntitiesPageParameters params = new SzEntitiesPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(null);
        params.setEntityIdBound("0");
        params.setPageSize(null);
        params.setSampleSize(null);
        
        SzEntitiesPage expected = getEntitiesPage(
            repoType,
            params.getEntityIdBound(),
            params.getBoundType(),
            params.getPageSize(),
            params.getSampleSize(),
            predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // try using entity ID bound as "max" with null bound type and
        // null page size defaulting to a multiple of the sample size
        params = new SzEntitiesPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(null);
        params.setEntityIdBound("max");
        params.setPageSize(null);
        params.setSampleSize(100);
        
        expected = getEntitiesPage(repoType,
                                   params.getEntityIdBound(),
                                   params.getBoundType(),
                                   params.getPageSize(),
                                   params.getSampleSize(),
                                   predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // try using entity ID bound as null with a lower bound
        params = new SzEntitiesPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(INCLUSIVE_LOWER);
        params.setEntityIdBound(null);
        params.setPageSize(5000);
        params.setSampleSize(null);
        
        expected = getEntitiesPage(repoType,
                                   params.getEntityIdBound(),
                                   params.getBoundType(),
                                   params.getPageSize(),
                                   params.getSampleSize(),
                                   predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // try using entity ID bound as null with an upper bound
        params = new SzEntitiesPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(EXCLUSIVE_UPPER);
        params.setEntityIdBound(null);
        params.setPageSize(5000);
        params.setSampleSize(null);
        
        expected = getEntitiesPage(repoType,
                                   params.getEntityIdBound(),
                                   params.getBoundType(),
                                   params.getPageSize(),
                                   params.getSampleSize(),
                                   predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // setup parameters for all-in-one page
        for (SzBoundType boundType : SzBoundType.values()) {
            params = new SzEntitiesPageParameters();
            params.setRepositoryType(repoType);
            params.setBoundType(boundType);
            params.setEntityIdBound(String.valueOf(
                boundType.isLower() ? 0L : Long.MAX_VALUE));
            params.setPageSize(5000);
            params.setSampleSize(null);
            
            expected = getEntitiesPage(repoType,
                                       params.getEntityIdBound(),
                                       params.getBoundType(),
                                       params.getPageSize(),
                                       params.getSampleSize(),
                                       predicate);

            params.setExpectedPage(expected);
            result.add(params);
        }

        // check if we have entities on the page
        if (filtered.size() > 4) {
            // determine a page size
            int pageCount = 4;
            int pageSize = filtered.size() / pageCount;
            while (pageSize < 4 && pageCount > 1) {
                pageCount--;
                pageSize = filtered.size() / pageCount;
            }
            if ((pageSize * pageCount) < filtered.size()) {
                pageSize++;
            }

            Integer[] sampleSizes = new Integer[(pageSize > 4) ? 2 : 1 ];
            sampleSizes[0] = null;
            if (sampleSizes.length > 1) {
                sampleSizes[1] = pageSize / 2;
            }

            for (SzBoundType boundType: SzBoundType.values()) {
                for (Integer sampleSize : sampleSizes) {
                    SzEntitiesPage lastExpected = null;
                    while (lastExpected == null 
                           || (boundType.isLower() && lastExpected.getPageMaximumValue() < maxEntityId)
                           || (boundType.isUpper() && lastExpected.getPageMinimumValue() > minEntityId))
                    {
                        params = new SzEntitiesPageParameters();
                        params.setRepositoryType(repoType);
                        params.setBoundType(boundType);

                        long newBound = (lastExpected == null)
                                ? (boundType.isLower() ? minEntityId : maxEntityId)
                                : (boundType.isLower() ? lastExpected.getPageMaximumValue() 
                                                       : lastExpected.getPageMinimumValue());

                        if (newBound < 0L) {
                            System.err.println();
                            System.err.println("ENTITY BOUND NEGATIVE: " 
                                + ((lastExpected == null) ? String.valueOf(newBound) 
                                : (lastExpected.getPageMinimumValue()
                                   + " / " + lastExpected.getPageMaximumValue())));
                        }
                        
                        params.setEntityIdBound(newBound < 0L ? null : String.valueOf(newBound));
                        params.setPageSize(pageSize);
                        params.setSampleSize(sampleSize);
            
                        expected = getEntitiesPage(
                            repoType,
                            params.getEntityIdBound(),
                            params.getBoundType(),
                            params.getPageSize(),
                            params.getSampleSize(),
                            predicate);

                        params.setExpectedPage(expected);
                        result.add(params);
                        lastExpected = expected;
                        if (lastExpected.getEntities().size() == 0) {
                            break;
                        }
                    }
                }
            }
        }

        return result;
    }

    public static record RelationPair(SzResolvedEntity  entity, 
                                      SzRelatedEntity   related,
                                      SzResolvedEntity  resolvedRelated)
    {
        // nothing to add
    }

    /**
     * Generates various relations page parameter combinations for the
     * specified {@link RepositoryType}, using the specified 
     * {@link Predicate} to qualify entities from the repository.
     * 
     * @param repoType The respective {@link RepositoryType}.
     * 
     * @param predicate The {@link Predicate} for qualifying related
     *                  entities from the repository.
     * 
     * @return The {@link List} of {@link SzRelationsPageParameters}
     *         to aid in testing the relations page retrieval.
     */
    public List<SzRelationsPageParameters> generateRelationsPageParameters(
        RepositoryType          repoType,
        Predicate<RelationPair> predicate)
    {
        List<SzRelationsPageParameters> result = new LinkedList<>();

        Repository repo = DataMartTestExtension.getRepository(repoType);

        SortedMap<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

        SortedMap<SzRelationKey, SzReportRelation> relations = new TreeMap<>();

        SzRelationKey minRelationKey = null;
        SzRelationKey maxRelationKey = null;
        // loop over the entities
        for (SzResolvedEntity entity : entities.values()) {
            long entityId = entity.getEntityId();
            
            // loop over the related entities
            for (SzRelatedEntity related : entity.getRelatedEntities().values()) {
                // get the related entity ID
                long relatedId = related.getEntityId();

                // create the relation key
                SzRelationKey key = new SzRelationKey(entityId, relatedId);
                
                RelationPair pair = new RelationPair(
                    entity, related, entities.get(related.getEntityId()));
                
                if (predicate.test(pair)) {
                    // track the minimum and maximum entity ID
                    if (minRelationKey == null || key.compareTo(minRelationKey) < 0) 
                    {
                        minRelationKey = key;
                    }
                    if (maxRelationKey == null || key.compareTo(maxRelationKey) > 0)
                    {
                        maxRelationKey = key;
                    }

                    SzReportRelation relation = new SzReportRelation();
                    
                    relation.setEntity(toReportEntity(entity));
                    relation.setRelatedEntity(toReportEntity(entities.get(relatedId)));
                    relation.setRelationType(SzRelationType.valueOf(related.getMatchType().toString()));
                    relation.setMatchKey(related.getMatchKey());
                    relation.setPrinciple(related.getPrinciple());

                    relations.put(key, relation);
                }
            }
        }

        // try using relation bound as "0:0" with null bound type and
        // null page size and null sample size (to use default page size)
        SzRelationsPageParameters params = new SzRelationsPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(null);
        params.setRelationBound("0:0");
        params.setPageSize(null);
        params.setSampleSize(null);
        
        SzRelationsPage expected = getRelationsPage(
            repoType,
            params.getRelationBound(),
            params.getBoundType(),
            params.getPageSize(),
            params.getSampleSize(),
            predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // try using relation bound as "max:max" with null bound type and
        // null page size defaulting to a multiple of the sample size
        params = new SzRelationsPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(null);
        params.setRelationBound("max:max");
        params.setPageSize(null);
        params.setSampleSize(100);
        
        expected = getRelationsPage(repoType,
                                    params.getRelationBound(),
                                    params.getBoundType(),
                                    params.getPageSize(),
                                    params.getSampleSize(),
                                    predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // try using relation bound as null with a lower bound
        params = new SzRelationsPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(INCLUSIVE_LOWER);
        params.setRelationBound(null);
        params.setPageSize(5000);
        params.setSampleSize(null);
        
        expected = getRelationsPage(repoType,
                                   params.getRelationBound(),
                                   params.getBoundType(),
                                   params.getPageSize(),
                                   params.getSampleSize(),
                                   predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // try using entity ID bound as null with an upper bound
        params = new SzRelationsPageParameters();
        params.setRepositoryType(repoType);
        params.setBoundType(EXCLUSIVE_UPPER);
        params.setRelationBound(null);
        params.setPageSize(5000);
        params.setSampleSize(null);
        
        expected = getRelationsPage(repoType,
                                    params.getRelationBound(),
                                    params.getBoundType(),
                                    params.getPageSize(),
                                    params.getSampleSize(),
                                    predicate);

        params.setExpectedPage(expected);
        result.add(params);

        // setup parameters for all-in-one page
        for (SzBoundType boundType : SzBoundType.values()) {
            params = new SzRelationsPageParameters();
            params.setRepositoryType(repoType);
            params.setBoundType(boundType);
            params.setRelationBound(boundType.isLower() ? "0:0" : "max:max");
            params.setPageSize(5000);
            params.setSampleSize(null);
            
            expected = getRelationsPage(repoType,
                                        params.getRelationBound(),
                                        params.getBoundType(),
                                        params.getPageSize(),
                                        params.getSampleSize(),
                                        predicate);

            params.setExpectedPage(expected);
            result.add(params);
        }

        // check if we have entities on the page
        if (relations.size() > 4) {
            // determine a page size
            int pageCount = 4;
            int pageSize = relations.size() / pageCount;
            while (pageSize < 4 && pageCount > 1) {
                pageCount--;
                pageSize = relations.size() / pageCount;
            }
            if ((pageSize * pageCount) < relations.size()) {
                pageSize++;
            }

            Integer[] sampleSizes = new Integer[(pageSize > 4) ? 2 : 1 ];
            sampleSizes[0] = null;
            if (sampleSizes.length > 1) {
                sampleSizes[1] = pageSize / 2;
            }

            for (SzBoundType boundType: SzBoundType.values()) {
                for (Integer sampleSize : sampleSizes) {
                    SzRelationsPage lastExpected = null;

                    while (lastExpected == null 
                           || (boundType.isLower() && maxRelationKey.compareTo(
                                    SzRelationKey.parse(lastExpected.getPageMaximumValue())) > 0)
                           || (boundType.isUpper() && minRelationKey.compareTo(
                                    SzRelationKey.parse(lastExpected.getPageMinimumValue())) < 0))
                    {
                        params = new SzRelationsPageParameters();
                        params.setRepositoryType(repoType);
                        params.setBoundType(boundType);

                        SzRelationKey newBound = (lastExpected == null)
                                ? (boundType.isLower() ? minRelationKey : maxRelationKey)
                                : SzRelationKey.parse(
                                    (boundType.isLower() 
                                        ? lastExpected.getPageMaximumValue()
                                        : lastExpected.getPageMinimumValue()));
                        if (newBound == null) {
                            System.err.println();
                            System.err.println("RELATION BOUND NULL: " 
                                + ((lastExpected == null) ? "NULL" 
                                : (lastExpected.getPageMinimumValue()
                                   + " / " + lastExpected.getPageMaximumValue())));
                        }
                        params.setRelationBound(
                            newBound == null ? null : newBound.toString());
                        params.setPageSize(pageSize);
                        params.setSampleSize(sampleSize);
            
                        expected = getRelationsPage(
                            repoType,
                            params.getRelationBound(),
                            params.getBoundType(),
                            params.getPageSize(),
                            params.getSampleSize(),
                            predicate);

                        params.setExpectedPage(expected);
                        result.add(params);
                        lastExpected = expected;
                        if (lastExpected.getRelations().size() == 0) {
                            break;
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * Validates an entities page.
     * 
     * @param testInfo
     * @param entityIdBound
     * @param boundType
     * @param pageSize
     * @param sampleSize
     * @param expected
     * @param actual
     */
    public void validateEntitiesPage(RepositoryType repoType,
                                     String         testInfo,
                                     String         entityIdBound,
                                     SzBoundType    boundType,
                                     Integer        pageSize,
                                     Integer        sampleSize,
                                     SzEntitiesPage expected,
                                     SzEntitiesPage actual)
        throws Exception
    {
        if (!expected.equals(actual)) {
            Repository repo = DataMartTestExtension.getRepository(repoType);
            Map<Long, SzResolvedEntity> loadedEntities = repo.getLoadedEntities();

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            if (sampleSize == null) {
                List<SzReportEntity> missing = new ArrayList<>(expected.getEntities());
                missing.removeAll(actual.getEntities());
                if (missing.size() > 0) {
                    pw.println();
                    pw.println("MISSING ENTITIES:");
                    for (SzReportEntity entity : missing) {
                        pw.println(" - - - - - - - - - - - - - - - - ");
                        pw.println("ENTITY ID: " + entity.getEntityId());
                        pw.println("--> REPORT ENTITY: ");
                        pw.println(toJsonText(parseValue(OBJECT_MAPPER.writeValueAsString(entity)), true));
                        pw.println();
                        pw.println("--> RESOLVED ENTITY: ");
                        SzResolvedEntity resolved = loadedEntities.get(entity.getEntityId());
                        pw.println(toJsonText(parseValue(resolved.toJsonText()), true));
                    }
                }
            }
            List<SzReportEntity> extra = new ArrayList<>(actual.getEntities());
            extra.removeAll(expected.getEntities());
            if (extra.size() > 0) {
                pw.println();
                pw.println("EXTRA ENTITIES:");
                for (SzReportEntity entity : extra) {
                    pw.println(" - - - - - - - - - - - - - - - - ");
                    pw.println("ENTITY ID: " + entity.getEntityId());
                    pw.println("--> REPORT ENTITY: ");
                    pw.println(toJsonText(parseValue(OBJECT_MAPPER.writeValueAsString(entity)), true));
                    pw.println();
                    pw.println("--> RESOLVED ENTITY: ");
                    SzResolvedEntity resolved = loadedEntities.get(entity.getEntityId());
                    pw.println(toJsonText(parseValue(resolved.toJsonText()), true));
                }
            }
            pw.flush();
            testInfo = validateReport(expected, actual, testInfo, false) + sw.toString();
        }
        
        assertEquals(expected.getBound(), actual.getBound(),
                    "Bound does not match: " + testInfo);
        assertEquals(expected.getBoundType(), actual.getBoundType(),
                        "Bound type does match: " + testInfo);
        assertEquals(expected.getPageSize(), actual.getPageSize(),
                        "Page size does not match: " + testInfo);
        assertEquals(expected.getSampleSize(), actual.getSampleSize(),
                        "Sample size does not match: " + testInfo);
        if (sampleSize == null) {
            assertEquals(expected.getMinimumValue(), actual.getMinimumValue(),
                            "Minimum values do not match: " + testInfo);
            assertEquals(expected.getMaximumValue(), actual.getMaximumValue(),
                            "Maximum values do not match: " + testInfo);

        } else if (expected.getEntities().size() > 0 
                   && actual.getEntities().size() > 0) 
        {
            assertTrue(expected.getMinimumValue() <= actual.getMinimumValue(),
                       "Actual minimum (" + actual.getMinimumValue() 
                       + ") is less than expected minimum (" 
                       + expected.getMinimumValue() + "): " + testInfo);
            
            assertTrue(expected.getMaximumValue() >= actual.getMaximumValue(),
                       "Actual maximum (" + actual.getMaximumValue() 
                       + ") is greater than expected maximum (" 
                       + expected.getMaximumValue() + "): " + testInfo);
        }

        assertEquals(expected.getPageMinimumValue(), actual.getPageMinimumValue(),
                        "Page minimum values do not match: " + testInfo);
        assertEquals(expected.getPageMaximumValue(), actual.getPageMaximumValue(),
                        "Page maximum values do not match: " + testInfo);
        assertEquals(expected.getAfterPageCount(), actual.getAfterPageCount(),
                        "After page counts do not match: " + testInfo);
        assertEquals(expected.getBeforePageCount(), actual.getBeforePageCount(),
                        "Before page counts do not match: " + testInfo);
        
        if (sampleSize == null) {
            List<SzReportEntity> missing = new ArrayList<>(expected.getEntities());
            missing.removeAll(actual.getEntities());
            assertEquals(0, missing.size(), 
                            "Some entities expected in page were missing: missing=[ "
                            + missing + " ], " + testInfo);
        }
        List<SzReportEntity> extra = new ArrayList<>(actual.getEntities());
        extra.removeAll(expected.getEntities());

        assertEquals(0, extra.size(),
            "Some entities retrieved in were not in expected "
            + "page: extra=[ " + extra + " ], " + testInfo);
    }

    /**
     * Validates an entities page.
     * 
     * @param testInfo
     * @param entityIdBound
     * @param boundType
     * @param pageSize
     * @param sampleSize
     * @param expected
     * @param actual
     */
    public void validateRelationsPage(RepositoryType    repoType,
                                      String            testInfo,
                                      String            relationBound,
                                      SzBoundType       boundType,
                                      Integer           pageSize,
                                      Integer           sampleSize,
                                      SzRelationsPage   expected,
                                      SzRelationsPage   actual)
        throws Exception
    {
        if (!expected.equals(actual)) {
            Repository repo = DataMartTestExtension.getRepository(repoType);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            if (sampleSize == null) {
                List<SzReportRelation> missing = new ArrayList<>(expected.getRelations());
                missing.removeAll(actual.getRelations());
                if (missing.size() > 0) {
                    pw.println();
                    pw.println("MISSING RELATIONS:");
                    for (SzReportRelation relation : missing) {
                        pw.println(" - - - - - - - - - - - - - - - - ");
                        pw.println("ENTITY ID: " + relation.getEntity().getEntityId());
                        pw.println("RELATED ID: " + relation.getRelatedEntity().getEntityId());
                        pw.println("--> REPORT RELATION: ");
                        pw.println(toJsonText(
                            parseValue(
                                OBJECT_MAPPER.writeValueAsString(relation)), true));
                    }
                }
            }

            List<SzReportRelation> extra = new ArrayList<>(actual.getRelations());
            extra.removeAll(expected.getRelations());
            if (extra.size() > 0) {
                pw.println();
                pw.println("EXTRA RELATIONS:");
                for (SzReportRelation relation : extra) {
                    pw.println(" - - - - - - - - - - - - - - - - ");
                    pw.println("ENTITY ID: " + relation.getEntity().getEntityId());
                    pw.println("RELATED ID: " + relation.getRelatedEntity().getEntityId());
                    pw.println("--> REPORT RELATION: ");
                    pw.println(toJsonText(
                        parseValue(
                            OBJECT_MAPPER.writeValueAsString(relation)), true));
                }
            }
            pw.flush();
            testInfo = validateReport(expected, actual, testInfo, false) + sw.toString();
        }
        
        assertEquals(expected.getBound(), actual.getBound(),
                    "Bound does not match: " + testInfo);
        assertEquals(expected.getBoundType(), actual.getBoundType(),
                        "Bound type does match: " + testInfo);
        assertEquals(expected.getPageSize(), actual.getPageSize(),
                        "Page size does not match: " + testInfo);
        assertEquals(expected.getSampleSize(), actual.getSampleSize(),
                        "Sample size does not match: " + testInfo);
        if (sampleSize == null) {
            assertEquals(expected.getMinimumValue(), actual.getMinimumValue(),
                            "Minimum values do not match: " + testInfo);
            assertEquals(expected.getMaximumValue(), actual.getMaximumValue(),
                            "Maximum values do not match: " + testInfo);

        } else if (expected.getRelations().size() > 0 
                   && actual.getRelations().size() > 0) 
        {
            SzRelationKey expectedMinKey = SzRelationKey.parse(expected.getMinimumValue());
            SzRelationKey actualMinKey   = SzRelationKey.parse(actual.getMinimumValue());

            assertTrue(expectedMinKey.compareTo(actualMinKey) <= 0, 
                       "Actual minimum (" + actual.getMinimumValue() 
                       + ") is less than expected minimum (" 
                       + expected.getMinimumValue() + "): " + testInfo);
            
            SzRelationKey expectedMaxKey = SzRelationKey.parse(expected.getMaximumValue());
            SzRelationKey actualMaxKey   = SzRelationKey.parse(actual.getMaximumValue());

            assertTrue(expectedMaxKey.compareTo(actualMaxKey) >= 0, 
                       "Actual maximum (" + actual.getMaximumValue() 
                       + ") is greater than expected maximum (" 
                       + expected.getMaximumValue() + "): " + testInfo);
        }

        assertEquals(expected.getPageMinimumValue(), actual.getPageMinimumValue(),
                        "Page minimum values do not match: " + testInfo);
        assertEquals(expected.getPageMaximumValue(), actual.getPageMaximumValue(),
                        "Page maximum values do not match: " + testInfo);
        assertEquals(expected.getAfterPageCount(), actual.getAfterPageCount(),
                        "After page counts do not match: " + testInfo);
        assertEquals(expected.getBeforePageCount(), actual.getBeforePageCount(),
                        "Before page counts do not match: " + testInfo);
        
        if (sampleSize == null) {
            List<SzReportRelation> missing = new ArrayList<>(expected.getRelations());
            missing.removeAll(actual.getRelations());
            assertEquals(0, missing.size(), 
                            "Some relations expected in page were missing: missing=[ "
                            + missing + " ], " + testInfo);
        }
        List<SzReportRelation> extra = new ArrayList<>(actual.getRelations());
        extra.removeAll(expected.getRelations());

        assertEquals(0, extra.size(),
            "Some relations retrieved in were not in expected "
            + "page: extra=[ " + extra + " ], " + testInfo);
    }

    /**
     * Gets an {@link SzEntitiesPage} for entities matching the specified
     * predicate from the respective repository.
     * 
     * @param repoType The {@link RepositoryType} to get the expected page result.
     * @param entityIdBound The bounded value for the returned entity ID's,
     *                      formatted as an integer or the word <code>"max"</code>.
     * @param boundType     The {@link SzBoundType} describing how the entity ID
     *                      bound value is applied in retrieving the page.
     * @param pageSize      The optional maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param predicate     The {@link Predicate} to qualify entities.
     * 
     * @return The expected {@link SzEntitiesPage} result (except containing all
     *         entities if a sample size is specified).
     */
    public SzEntitiesPage getEntitiesPage(RepositoryType                repoType,
                                          String                        entityIdBound,
                                          SzBoundType                   boundType,
                                          Integer                       pageSize,
                                          Integer                       sampleSize,
                                          Predicate<SzResolvedEntity>   predicate)
    {
        // default the bound type
        if (boundType == null) {
            boundType = ("max".equals(entityIdBound)) ? EXCLUSIVE_UPPER : EXCLUSIVE_LOWER;
        }

        long boundValue = 0L;
        // default the bound if not specified
        if (entityIdBound == null) {
            boundValue = (boundType.isLower()) ? 0L : Long.MAX_VALUE;
            entityIdBound = (boundType.isLower()) ? "0" : "max";

        } else if ("max".equals(entityIdBound.trim().toLowerCase())) {
            boundValue = Long.MAX_VALUE;
            entityIdBound = entityIdBound.trim().toLowerCase();

        } else {
            try {
                boundValue = Long.parseLong(entityIdBound.trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "The entity ID bound must either be an integer or \"max\": "
                    + entityIdBound);
            }
        }

        // normalize the page size
        if (pageSize == null && sampleSize == null) {
            pageSize = ReportUtilities.DEFAULT_PAGE_SIZE;
        } else if (pageSize == null) {
            pageSize = ReportUtilities.SAMPLE_SIZE_MULTIPLIER * sampleSize;
        }

        SzEntitiesPage page = new SzEntitiesPage();
        page.setBound(entityIdBound);
        page.setBoundType(boundType);
        page.setPageSize(pageSize);
        page.setSampleSize(sampleSize);

        // get the repo
        Repository repo = DataMartTestExtension.getRepository(repoType);

        // get the entities
        SortedMap<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

        List<SzResolvedEntity> entityList = new ArrayList<>(entities.values());
        List<SzReportEntity> reportEntities = new LinkedList<>();

        // check the bound type
        if (boundType.isUpper()) {
            Collections.reverse(entityList);
        }

        // track the min and max tracking and result count
        long minEntityId        = -1L;
        long maxEntityId        = -1L;
        long afterPageCount     = 0;
        long beforePageCount    = 0;
        long totalCount         = 0;

        // iterate over the entities
        for (SzResolvedEntity entity : entityList) {
            // get the entity
            long entityId = entity.getEntityId();

            // skip any entities failing the predicate
            if (!predicate.test(entity)) {
                continue;
            }

            // increment the total count of matching predicate
            totalCount++;

            // check the bound
            if (!boundType.checkSatisfies(entityId, boundValue))
            {
                if (boundType.isUpper()) {
                    afterPageCount++;
                } else {
                    beforePageCount++;
                }
                continue;
            }

            // check if we have filled the page
            if (reportEntities.size() >= pageSize)
            {
                if (boundType.isLower()) {
                    afterPageCount++;
                } else {
                    beforePageCount++;
                }
                continue;
            }

            // convert the entity to a report entity
            SzReportEntity reportEntity = toReportEntity(entity);

            // add to the report entities list
            if (boundType.isUpper()) {
                // add at the front to reverse order if upper bound
                reportEntities.add(0, reportEntity);
            } else {
                // add at the end if a lower bound
                reportEntities.add(reportEntity);
            }

            // track the minimum and maximum entity ID
            if (minEntityId < 0L || entityId < minEntityId) 
            {
                minEntityId = entityId;
            }
            if (maxEntityId < 0L || entityId > maxEntityId) 
            {
                maxEntityId = entityId;
            }
        }

        // if sampling, set the page min and max (otherwise these should
        // automatically use the standard min and max for the page)
        if (sampleSize != null && reportEntities.size() > sampleSize) {
            page.setPageMinimumValue(minEntityId);
            page.setPageMaximumValue(maxEntityId);
        }

        // even if we have a sample size, we are using all entities
        // samples are random and we cannot use equality check to 
        // ensure correctness
        page.setEntities(reportEntities);

        // set the stats for knowing how many pages come before and after
        page.setTotalEntityCount(totalCount);
        page.setBeforePageCount(beforePageCount);
        page.setAfterPageCount(afterPageCount);

        return page;
    }

    /**
     * Gets an {@link SzEntitiesPage} for entities matching the specified
     * predicate from the respective repository.
     * 
     * @param repoType The {@link RepositoryType} to get the expected page result.
     * @param relationBound The bounded value for the returned relations.
     * @param boundType     The {@link SzBoundType} describing how the entity ID
     *                      bound value is applied in retrieving the page.
     * @param pageSize      The optional maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param predicate     The {@link Predicate} to qualify entities.
     * 
     * @return The expected {@link SzRelationsPage} result (except containing all
     *         relations even if a sample size is specified).
     */
    public SzRelationsPage getRelationsPage(
        RepositoryType          repoType,
        String                  relationBound,
        SzBoundType             boundType,
        Integer                 pageSize,
        Integer                 sampleSize,
        Predicate<RelationPair> predicate)
    {
        // default the bound type
        if (boundType == null) {
            boundType = ("max:max".equals(relationBound)) 
                ? EXCLUSIVE_UPPER : EXCLUSIVE_LOWER;
        }

        SzRelationKey boundValue = null;
        // default the bound if not specified
        if (relationBound == null) {
            boundValue = (boundType.isLower()) 
                ? new SzRelationKey(0L, 0L)
                : new SzRelationKey(Long.MAX_VALUE, Long.MAX_VALUE);
            
            relationBound = (boundType.isLower()) ? "0:0" : "max:max";

        } else if ("max:max".equals(relationBound.trim().toLowerCase())) {
            boundValue = new SzRelationKey(Long.MAX_VALUE, Long.MAX_VALUE);
            relationBound = "max:max";

        } else {
            boundValue = SzRelationKey.parse(relationBound);
        }

        // normalize the page size
        if (pageSize == null && sampleSize == null) {
            pageSize = ReportUtilities.DEFAULT_PAGE_SIZE;
        } else if (pageSize == null) {
            pageSize = ReportUtilities.SAMPLE_SIZE_MULTIPLIER * sampleSize;
        }

        SzRelationsPage page = new SzRelationsPage();
        page.setBound(relationBound);
        page.setBoundType(boundType);
        page.setPageSize(pageSize);
        page.setSampleSize(sampleSize);

        // get the repo
        Repository repo = DataMartTestExtension.getRepository(repoType);

        // get the entities
        SortedMap<Long, SzResolvedEntity> entities = repo.getLoadedEntities();

        List<RelationPair> relationPairs  = new LinkedList<>();

        for (SzResolvedEntity entity : entities.values()) {
            for (SzRelatedEntity related : entity.getRelatedEntities().values()) {
                relationPairs.add(new RelationPair(
                    entity, related, entities.get(related.getEntityId())));
            }
        }

        List<SzReportRelation> reportRelations = new LinkedList<>();

        // check the bound type
        if (boundType.isUpper()) {
            Collections.reverse(relationPairs);
        }

        // track the min and max tracking and result count
        SzRelationKey   minRelationKey  = null;
        SzRelationKey   maxRelationKey  = null;
        long            afterPageCount  = 0;
        long            beforePageCount = 0;
        long            totalCount      = 0;

        // iterate over the entities
        for (RelationPair pair : relationPairs) {
            // skip any entities failing the predicate
            if (!predicate.test(pair)) {
                continue;
            }

            // get the entity and related entity
            SzResolvedEntity    entity  = pair.entity();
            SzRelatedEntity     related = pair.related();

            // get the entity ID
            long entityId   = entity.getEntityId();
            long relatedId  = related.getEntityId();
            
            SzRelationKey key = new SzRelationKey(entityId, relatedId);

            // increment the total count of matching predicate
            totalCount++;

            // check the bound
            if (!boundType.checkSatisfies(key, boundValue))
            {
                if (boundType.isUpper()) {
                    afterPageCount++;
                } else {
                    beforePageCount++;
                }
                continue;
            }

            // check if we have filled the page
            if (reportRelations.size() >= pageSize)
            {
                if (boundType.isLower()) {
                    afterPageCount++;
                } else {
                    beforePageCount++;
                }
                continue;
            }

            // convert the entities to report entities and to a report relation
            SzReportEntity      reportEntity    = toReportEntity(entity);
            SzReportEntity      reportRelated   = toReportEntity(entities.get(relatedId));
            SzReportRelation    reportRelation  = new SzReportRelation();
            
            reportRelation.setEntity(reportEntity);
            reportRelation.setRelatedEntity(reportRelated);
            reportRelation.setRelationType(SzRelationType.valueOf(related.getMatchType().toString()));
            reportRelation.setMatchKey(related.getMatchKey());
            reportRelation.setPrinciple(related.getPrinciple());

            // add to the report entities list
            if (boundType.isUpper()) {
                // add at the front to reverse order if upper bound
                reportRelations.add(0, reportRelation);
            } else {
                // add at the end if a lower bound
                reportRelations.add(reportRelation);
            }

            // track the minimum and maximum entity ID
            if (minRelationKey == null || key.compareTo(minRelationKey) < 0) 
            {
                minRelationKey = key;
            }
            if (maxRelationKey == null || key.compareTo(maxRelationKey) > 0)
            {
                maxRelationKey = key;
            }
        }

        // if sampling, set the page min and max (otherwise these should
        // automatically use the standard min and max for the page)
        if (sampleSize != null && reportRelations.size() > sampleSize) {
            page.setPageMinimumValue(minRelationKey != null ? minRelationKey.toString() : null);
            page.setPageMaximumValue(maxRelationKey != null ? maxRelationKey.toString() : null);
        }

        // even if we have a sample size, we are using all entities
        // samples are random and we cannot use equality check to 
        // ensure correctness
        page.setRelations(reportRelations);

        // set the stats for knowing how many pages come before and after
        page.setTotalRelationCount(totalCount);
        page.setBeforePageCount(beforePageCount);
        page.setAfterPageCount(afterPageCount);

        return page;
    }    

    /**
     * Creates an {@link SzReportEntity} from an {@link SzResolvedEntity}.
     * 
     * @param resolvedEntity The source {@link SzResolvedEntity}.
     * 
     * @return The created {@link SzReportEntity}.
     */
    public static SzReportEntity toReportEntity(SzResolvedEntity resolvedEntity)
    {
        SzReportEntity reportEntity = new SzReportEntity();
        
        int recordCount = resolvedEntity.getRecords().size();

        reportEntity.setEntityId(resolvedEntity.getEntityId());
        reportEntity.setEntityName(resolvedEntity.getEntityName());
        reportEntity.setRecordCount(recordCount);
        reportEntity.setRelationCount(resolvedEntity.getRelatedEntities().size());

        List<SzReportRecord> reportRecords = new ArrayList<>(recordCount);

        for (SzRecord record : resolvedEntity.getRecords().values()) {
            // create the report record
            SzReportRecord reportRecord = toReportRecord(record);

            // add to the list
            reportRecords.add(reportRecord);
        }

        reportEntity.setRecords(reportRecords);

        return reportEntity;
    }

    /**
     * Creates an {@link SzReportRecord} from an {@link SzRecord}.
     * 
     * @param record The source {@link SzRecord}.
     * 
     * @return The created {@link SzReportRecord}.
     */
    public static SzReportRecord toReportRecord(SzRecord record) {
        SzReportRecord reportRecord = new SzReportRecord();

        reportRecord.setDataSource(record.getDataSource());
        reportRecord.setRecordId(record.getRecordId());
        reportRecord.setMatchKey(record.getMatchKey());
        reportRecord.setPrinciple(record.getPrinciple());
        
        return reportRecord;
    }

    /**
     * Creates a {@link Map} of {@link RepositoryType} keys to 
     * {@link SzSummaryStats} values describing the expected
     * summary stats reports for the respective repository.
     * 
     * @return A {@link Map} of {@link RepositoryType} keys to 
     *         {@link SzSummaryStats} values describing the expected
     *         summary stats reports for the respective repository.
     */
    public Map<RepositoryType, SzSummaryStats> getSummaryStats() {
        Map<RepositoryType, SzSummaryStats> result = new TreeMap<>();

        RepositoryType[] repoTypes = RepositoryType.values();

        for (RepositoryType repoType : repoTypes) {
            SzSummaryStats stats = getSummaryStats(repoType);

            result.put(repoType, stats);
        }

        return result;
    }

    /**
     * Calculates the summary stats for the specified repository type.
     * 
     * @param repoType The {@link RepositoryType} for the repository.
     * 
     * @return The {@link SzSummaryStats} for the repository.
     */
    public SzSummaryStats getSummaryStats(RepositoryType repoType) {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        
        Map<Long, SzResolvedEntity> loadedEntities = repo.getLoadedEntities();

        Map<String, SzSourceSummary> sourceSummaries = new TreeMap<>();
        Map<CrossSourceKey, SzCrossSourceSummary> crossSummaries = new TreeMap<>();

        Map<CrossMatchKey, SzMatchCounts>       matchMap    = new TreeMap<>();
        Map<CrossRelationKey, SzRelationCounts> relationMap = new TreeMap<>();

        // iterate over the entities
        for (SzResolvedEntity entity : loadedEntities.values()) {

            // get the record summary and records
            Map<String, Integer> recordSummary = entity.getSourceSummary();
            Map<SzRecordKey, SzRecord> records = entity.getRecords();

            // calculate the match pairs
            SortedSet<MatchPairKey> matchPairs = new TreeSet<>();
            // always account for the null / null value
            matchPairs.add(new MatchPairKey(null, null));
            for (SzRecord record : records.values()) {
                String matchKey = record.getMatchKey();
                String principle = record.getPrinciple();
                if (matchKey == null || principle == null) {
                    continue;
                }
                matchPairs.add(new MatchPairKey(matchKey, principle));
                matchPairs.add(new MatchPairKey(matchKey, null));
                matchPairs.add(new MatchPairKey(null, principle));
            }

            // iterate over the record summary
            recordSummary.forEach((source1, recordCount1) -> {
                // update the base stats for the source summary object
                SzSourceSummary srcSummary = sourceSummaries.get(source1);
                if (srcSummary == null) {
                    srcSummary = new SzSourceSummary();
                    srcSummary.setDataSource(source1);
                    sourceSummaries.put(source1, srcSummary);
                }
            
                srcSummary.setEntityCount(srcSummary.getEntityCount() + 1);
                srcSummary.setRecordCount(srcSummary.getRecordCount() + recordCount1);
                if (records.size() == 1) {
                    srcSummary.setUnmatchedRecordCount(
                        srcSummary.getUnmatchedRecordCount() + 1);
                }

                // iterate over the cross match keys
                recordSummary.forEach((source2, recordCount2) -> {
                    matchPairs.forEach(matchPairKey -> {
                        String matchKey = matchPairKey.getMatchKey();
                        String principle = matchPairKey.getPrinciple();

                        CrossMatchKey crossKey = new CrossMatchKey(
                            source1, source2, matchKey, principle);

                        // check if we have different data sources OR two 
                        // or more records from the same data source
                        if (!source1.equals(source2) || recordCount1 > 1) {
                            SzMatchCounts matchCounts = matchMap.get(crossKey);
                            if (matchCounts == null) {
                                matchCounts = new SzMatchCounts(matchKey, principle);
                                matchMap.put(crossKey, matchCounts);
                            }

                            matchCounts.setEntityCount(matchCounts.getEntityCount() + 1);
                            matchCounts.setRecordCount(
                                matchCounts.getRecordCount() + recordCount1);
                        }
                    });
                });
            });

            // determine the cross relation keys for this entity
            Set<CrossRelationKey> crossRelationKeys = new TreeSet<>();
            for (SzRelatedEntity related : entity.getRelatedEntities().values()) {
                // get the match key and principle
                String              matchKey    = related.getMatchKey();
                String              principle   = related.getPrinciple();
                SzMatchType         matchType   = related.getMatchType();
                SzResolvedEntity    resolvedRel = loadedEntities.get(related.getEntityId());

                String revMatchKey = resolvedRel.getRelatedEntities().get(
                    entity.getEntityId()).getMatchKey();

                Map<String, Integer> relatedSummary = related.getSourceSummary();

                recordSummary.forEach((source1, recordCount1) -> {
                    relatedSummary.forEach((source2, relatedCount2) -> {
                        SourceRelationKey.variants(
                            matchType, matchKey, revMatchKey,principle).forEach(srk -> 
                        {
                            CrossRelationKey crossKey 
                                = new CrossRelationKey(source1, 
                                                       source2,
                                                       matchType,
                                                       srk.getMatchKey(),
                                                       srk.getPrinciple());

                            crossRelationKeys.add(crossKey);

                            SzRelationCounts relCounts = relationMap.get(crossKey);
                            if (relCounts == null) {
                                relCounts = new SzRelationCounts(
                                    srk.getMatchKey(), srk.getPrinciple());
                                relationMap.put(crossKey, relCounts);
                            }

                            relCounts.setRelationCount(
                                relCounts.getRelationCount() + 1);
                            });
                    });
                });
            }
            for (CrossRelationKey crossKey : crossRelationKeys) {
                // get the relation counts which cannot be null at this pout
                SzRelationCounts relCounts = relationMap.get(crossKey);

                // get the record count for the first entity on source 1
                int recordCount = recordSummary.get(crossKey.getSource1());

                // increment the entity and record counts just once per
                // cross relation key -- NOT once per related entity
                relCounts.setEntityCount(relCounts.getEntityCount() + 1);
                relCounts.setRecordCount(relCounts.getRecordCount() + recordCount);
            }
        }

        // add the match counts to the cross summaries
        matchMap.forEach((crossKey, matchCount) -> {
            String source1 = crossKey.getSource1();
            String source2 = crossKey.getSource2();

            CrossSourceKey csk = new CrossSourceKey(source1, source2);
            SzCrossSourceSummary crossSummary = crossSummaries.get(csk);
            if (crossSummary == null) {
                crossSummary = new SzCrossSourceSummary(source1, source2);
                crossSummaries.put(csk, crossSummary);
            }
            
            crossSummary.addMatches(matchCount);
        });

        // add the relation counts to the cross summaries
        relationMap.forEach((crossKey, relationCount) -> {
            String      source1     = crossKey.getSource1();
            String      source2     = crossKey.getSource2();
            SzMatchType matchType   = crossKey.getMatchType();

            CrossSourceKey csk = new CrossSourceKey(source1, source2);
            SzCrossSourceSummary crossSummary = crossSummaries.get(csk);
            if (crossSummary == null) {
                crossSummary = new SzCrossSourceSummary(source1, source2);
                crossSummaries.put(csk, crossSummary);
            }
            
            switch (matchType) {
                case AMBIGUOUS_MATCH:
                    crossSummary.addAmbiguousMatches(relationCount);
                    break;

                case POSSIBLE_MATCH:
                    crossSummary.addPossibleMatches(relationCount);
                    break;

                case POSSIBLE_RELATION:
                    crossSummary.addPossibleRelations(relationCount);
                    break;

                case DISCLOSED_RELATION:
                    crossSummary.addDisclosedRelations(relationCount);
                    break;

                default:
                    throw new IllegalStateException(
                        "Unrecognized match type; " + matchType);
            }
        });

        // add the cross summaries to the source summaries
        crossSummaries.forEach((crossKey, crossSummary) -> {
            if (crossSummary.getAmbiguousMatches().size() == 0) {
                crossSummary.addAmbiguousMatches(new SzRelationCounts());
            }
            if (crossSummary.getPossibleMatches().size() == 0) {
                crossSummary.addPossibleMatches(new SzRelationCounts());
            }
            if (crossSummary.getPossibleRelations().size() == 0) {
                crossSummary.addPossibleRelations(new SzRelationCounts());
            }
            if (crossSummary.getDisclosedRelations().size() == 0) {
                crossSummary.addDisclosedRelations(new SzRelationCounts());
            }
            
            String source1 = crossKey.getSource1();
            
            SzSourceSummary sourceSummary = sourceSummaries.get(source1);

            sourceSummary.addCrossSourceSummary(crossSummary);
        });

        // add the source summaries to t a summary stats
        SzSummaryStats summaryStats = new SzSummaryStats();
        summaryStats.setSourceSummaries(sourceSummaries.values());

        // return the summary stats
        return summaryStats;
    }

    /**
     * Validates the specified actual report object matches the
     * specified expected report object.
     * 
     * @param <R>       The report type.
     * @param expected  The expected report value.
     * @param actual    The actual report value.
     * @param testInfo  The diagnostic {@link String} describing the test.
     */
    public static <R> void validateReport(R         expected, 
                                          R         actual,
                                          String    testInfo) 
        throws Exception
    {
        validateReport(expected,
                       actual,
                       expected.getClass().getSimpleName(),
                       testInfo);
    }

    /**
     * Validates the specified actual report object matches the
     * specified expected report object.
     * 
     * @param <R>       The report type.
     * @param expected  The expected report value.
     * @param actual    The actual report value.
     * @param testInfo  The diagnostic {@link String} describing the test.
     * @param triggerFail   <code>true</code> if a failure should be triggered
     *                      if the validation fails, otherwise false.
     */
    public static <R> String validateReport(R       expected, 
                                            R       actual,
                                            String  testInfo,
                                            boolean triggerFail)
        throws Exception
    {
        return validateReport(expected,
                              actual,
                              expected.getClass().getSimpleName(),
                              testInfo,
                              triggerFail);
    }

    /**
     * Validates the specified actual report object matches the
     * specified expected report object.
     * 
     * @param <R>           The report type.
     * @param expected      The expected report value.
     * @param actual        The actual report value.
     * @param reportType    The types of the report for messaging.
     * @param testInfo      The diagnostic {@link String} describing the test.
     */
    public static <R> void validateReport(R         expected, 
                                          R         actual,
                                          String    reportType,
                                          String    testInfo) 
        throws Exception
    {
        validateReport(expected, actual, reportType, testInfo, false);
    }

    /**
     * Validates the specified actual report object matches the
     * specified expected report object.
     * 
     * @param <R>           The report type.
     * @param expected      The expected report value.
     * @param actual        The actual report value.
     * @param reportType    The types of the report for messaging.
     * @param testInfo      The diagnostic {@link String} describing the test.
     * @param triggerFail   <code>true</code> if a failure should be triggered
     *                      if the validation fails, otherwise false.
     * 
     * @return <code>null</code> if validated successfully, otherwise 
     *         the failure info indicating where the JSON was logged.
     */
    public static <R> String validateReport(R       expected, 
                                            R       actual,
                                            String  reportType,
                                            String  testInfo,
                                            boolean triggerFail)
        throws Exception
    {
        if (!expected.equals(actual)) {
            File expectedFile = File.createTempFile("expected-", ".json");
            File actualFile = new File(
                expectedFile.getParent(),
                expectedFile.getName().replace("expected-", "actual-"));

            try (FileOutputStream fos = new FileOutputStream(expectedFile);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8);
                    PrintWriter pw = new PrintWriter(osw))
            {
                pw.println(toJsonText(parseValue(
                    OBJECT_MAPPER.writeValueAsString(expected)), true));
                pw.flush();
            }

            try (FileOutputStream fos = new FileOutputStream(actualFile);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8);
                    PrintWriter pw = new PrintWriter(osw))
            {
                pw.println(toJsonText(parseValue(
                    OBJECT_MAPPER.writeValueAsString(actual)), true));
                pw.flush();
            }

            testInfo = testInfo + ", expectedFile=[ " + expectedFile.getAbsolutePath()
                    + " ], actualFile=[ " + actualFile.getAbsolutePath() + " ]";
            if (triggerFail) {
                fail("The " + reportType + " report does not match the expected report: "
                    + testInfo);
            } else {
                return testInfo;
            }
        }    
        return null;        
    }

    /**
     * Validates the specified {@link Throwable} was expected and 
     * is of the expected type (or its cause is of the expected type).
     * 
     * @param testInfo The test info for logging.
     * @param t The {@link Throwable} to validate.
     * @param exceptionType The expected exception type or <code>null</code>
     *                      if none expected.
     */
    public static void validateException(String testInfo, Throwable t, Class<?> exceptionType) {
        if ((exceptionType == null) || !checkExceptionChain(t, exceptionType)) {
            fail("Unexpected exception (" + t.getClass().getName()
                 + ") when expecting "
                 + (exceptionType == null ? "none" : exceptionType.getName())
                 + ": " + testInfo, t);
        }
    }

    /**
     * Checks the exception recursively to see if one cause in its chain matches
     * the specified type.
     * 
     * @param t The {@link Throwable} to check.
     * @param exceptionType The non-null exception type
     * 
     * @return <code>true</code> if expected, and <code>false</code> if not.
     */
    public static boolean checkExceptionChain(Throwable t, Class<?> exceptionType) {
        while (t != null) {
            if (exceptionType.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }

        // if we get here return false
        return false;
    }
}
