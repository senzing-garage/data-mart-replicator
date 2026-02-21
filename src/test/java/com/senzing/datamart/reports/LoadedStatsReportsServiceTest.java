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
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzLoadedStats;
import com.senzing.datamart.reports.model.SzSourceLoadedStats;
import com.senzing.sql.ConnectionProvider;

/**
 * Tests the {@link LoadedStatsReportsService} interface methods using
 * {@link TestLoadedStatsReportsService}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class LoadedStatsReportsServiceTest extends LoadedStatsReportsTest {

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
     * Overridden to use {@link TestLoadedStatsReportsService} to obtain the result.
     * The {@link DataSourceCombination} is interpreted as follows:
     * <ul>
     *   <li>{@link DataSourceCombination#LOADED} → {@code onlyLoaded = true}</li>
     *   <li>{@link DataSourceCombination#ALL_BUT_DEFAULT} → {@code onlyLoaded = false}</li>
     * </ul>
     */
    @Override
    protected SzLoadedStats getLoadedStatistics(RepositoryType          repoType,
                                                ConnectionProvider      connProvider,
                                                DataSourceCombination   sourceCombination,
                                                Set<String>             dataSources)
        throws Exception
    {
        // Determine the onlyLoaded flag based on the DataSourceCombination
        boolean onlyLoaded;
        switch (sourceCombination) {
            case LOADED:
                onlyLoaded = true;
                break;
            case ALL_BUT_DEFAULT:
                onlyLoaded = false;
                break;
            default:
                fail("Unexpected DataSourceCombination for service test: "
                     + sourceCombination);
                return null; // unreachable, but required for compilation
        }

        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestLoadedStatsReportsService service
            = new TestLoadedStatsReportsService(connProvider, repo);
        return service.getLoadedStatistics(onlyLoaded);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestLoadedStatsReportsService} to obtain the result.
     */
    @Override
    protected SzSourceLoadedStats getSourceLoadedStatistics(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestLoadedStatsReportsService service
            = new TestLoadedStatsReportsService(connProvider, repo);
        return service.getSourceLoadedStatistics(dataSource);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestLoadedStatsReportsService} to obtain the result.
     */
    @Override
    protected SzEntitiesPage getEntityIdsForDataSource(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        String             dataSource,
        String             entityIdBound,
        SzBoundType        boundType,
        Integer            pageSize,
        Integer            sampleSize)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestLoadedStatsReportsService service
            = new TestLoadedStatsReportsService(connProvider, repo);
        return service.getEntityIdsForDataSource(
            dataSource, entityIdBound, boundType, pageSize, sampleSize);
    }
}
