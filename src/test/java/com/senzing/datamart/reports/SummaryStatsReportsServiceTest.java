package com.senzing.datamart.reports;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzCrossSourceMatchCounts;
import com.senzing.datamart.reports.model.SzCrossSourceRelationCounts;
import com.senzing.datamart.reports.model.SzCrossSourceSummary;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzRelationsPage;
import com.senzing.datamart.reports.model.SzSourceSummary;
import com.senzing.datamart.reports.model.SzSummaryStats;
import com.senzing.sql.ConnectionProvider;

/**
 * Tests the {@link SummaryStatsReportsService} interface methods using
 * {@link TestSummaryStatsReportsService}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class SummaryStatsReportsServiceTest extends SummaryStatsReportsTest {

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return only {@link DataSourceCombination#LOADED} and
     * {@link DataSourceCombination#ALL_BUT_DEFAULT} since the service interface
     * only supports these two modes via its {@code onlyLoaded} boolean parameter.
     */
    @Override
    protected Set<DataSourceCombination> getDataSourceCombinations() {
        return Collections.unmodifiableSet(
            EnumSet.of(DataSourceCombination.LOADED,
                       DataSourceCombination.ALL_BUT_DEFAULT));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return 100 for spot-checking since the base class handles
     * comprehensive correctness verification.
     */
    @Override
    protected int getSkipFactor() {
        return 100;
    }

    /**
     * Converts a {@link DataSourceCombination} to a boolean {@code onlyLoaded} value.
     *
     * @param sourceCombo The {@link DataSourceCombination} to convert.
     * @return {@code true} for {@link DataSourceCombination#LOADED}, {@code false}
     *         for {@link DataSourceCombination#ALL_BUT_DEFAULT}.
     * @throws AssertionError If the {@link DataSourceCombination} is not supported.
     */
    private boolean toOnlyLoaded(DataSourceCombination sourceCombo) {
        switch (sourceCombo) {
            case LOADED:
                return true;
            case ALL_BUT_DEFAULT:
                return false;
            default:
                fail("Unexpected DataSourceCombination for service test: " + sourceCombo);
                return false; // unreachable, but required for compilation
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzSummaryStats getSummaryStatistics(RepositoryType         repoType,
                                                  ConnectionProvider     connProvider,
                                                  String                 matchKey,
                                                  String                 principle,
                                                  DataSourceCombination  sourceCombo,
                                                  Set<String>            dataSources)
        throws Exception
    {
        boolean onlyLoaded = toOnlyLoaded(sourceCombo);
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryStats(matchKey, principle, onlyLoaded);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzSourceSummary getSourceSummary(RepositoryType        repoType,
                                               ConnectionProvider    connProvider,
                                               String                dataSource,
                                               String                matchKey,
                                               String                principle,
                                               DataSourceCombination sourceCombo,
                                               Set<String>           dataSources)
        throws Exception
    {
        boolean onlyLoaded = toOnlyLoaded(sourceCombo);
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSourceSummary(dataSource, matchKey, principle, onlyLoaded);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzCrossSourceSummary getCrossSourceSummary(RepositoryType     repoType,
                                                         ConnectionProvider connProvider,
                                                         String             dataSource,
                                                         String             vsDataSource,
                                                         String             matchKey,
                                                         String             principle)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getCrossSourceSummary(dataSource, vsDataSource, matchKey, principle);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzCrossSourceMatchCounts getCrossSourceMatchSummary(RepositoryType     repoType,
                                                                  ConnectionProvider connProvider,
                                                                  String             dataSource,
                                                                  String             vsDataSource,
                                                                  String             matchKey,
                                                                  String             principle)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getCrossSourceMatchSummary(dataSource, vsDataSource, matchKey, principle);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzCrossSourceRelationCounts getCrossSourceAmbiguousMatchSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getCrossSourceAmbiguousMatchSummary(
            dataSource, vsDataSource, matchKey, principle);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzCrossSourceRelationCounts getCrossSourcePossibleMatchSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getCrossSourcePossibleMatchSummary(
            dataSource, vsDataSource, matchKey, principle);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzCrossSourceRelationCounts getCrossSourcePossibleRelationSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getCrossSourcePossibleRelationSummary(
            dataSource, vsDataSource, matchKey, principle);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
    protected SzCrossSourceRelationCounts getCrossSourceDisclosedRelationSummary(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             vsDataSource,
        String             matchKey,
        String             principle)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getCrossSourceDisclosedRelationSummary(
            dataSource, vsDataSource, matchKey, principle);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryMatchEntityIds(
            dataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryAmbiguousMatchEntityIds(
            dataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryPossibleMatchEntityIds(
            dataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryPossibleRelationEntityIds(
            dataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryDisclosedRelatedEntityIds(
            dataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryMatchEntityIds(
            dataSource, vsDataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryAmbiguousMatchEntityIds(
            dataSource, vsDataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryPossibleMatchEntityIds(
            dataSource, vsDataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryPossibleRelationEntityIds(
            dataSource, vsDataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryDisclosedRelationEntityIds(
            dataSource, vsDataSource, matchKey, principle, entityIdBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryAmbiguousMatchRelations(
            dataSource, vsDataSource, matchKey, principle, relationBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryPossibleMatchRelations(
            dataSource, vsDataSource, matchKey, principle, relationBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryPossibleRelations(
            dataSource, vsDataSource, matchKey, principle, relationBound, boundType, pageSize, sampleSize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestSummaryStatsReportsService} to obtain the result.
     */
    @Override
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
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestSummaryStatsReportsService service
            = new TestSummaryStatsReportsService(connProvider, repo);
        return service.getSummaryDisclosedRelations(
            dataSource, vsDataSource, matchKey, principle, relationBound, boundType, pageSize, sampleSize);
    }
}
