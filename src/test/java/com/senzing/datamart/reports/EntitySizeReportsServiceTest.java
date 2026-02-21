package com.senzing.datamart.reports;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.sql.ConnectionProvider;

/**
 * Tests the {@link EntitySizeReportsService} interface methods using
 * {@link TestEntitySizeReportsService}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class EntitySizeReportsServiceTest extends EntitySizeReportsTest {

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestEntitySizeReportsService} to obtain the result.
     */
    @Override
    protected SzEntitySizeBreakdown getEntitySizeBreakdown(RepositoryType     repoType,
                                                           ConnectionProvider connProvider)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestEntitySizeReportsService service
            = new TestEntitySizeReportsService(connProvider, repo);
        return service.getEntitySizeBreakdown();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestEntitySizeReportsService} to obtain the result.
     */
    @Override
    protected SzEntitySizeCount getEntitySizeCount(RepositoryType     repoType,
                                                   ConnectionProvider connProvider,
                                                   int                entitySize)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestEntitySizeReportsService service
            = new TestEntitySizeReportsService(connProvider, repo);
        return service.getEntitySizeCount(entitySize);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestEntitySizeReportsService} to obtain the result.
     */
    @Override
    protected SzEntitiesPage getEntityIdsForEntitySize(RepositoryType     repoType,
                                                       ConnectionProvider connProvider,
                                                       int                entitySize,
                                                       String             entityIdBound,
                                                       SzBoundType        boundType,
                                                       Integer            pageSize,
                                                       Integer            sampleSize)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestEntitySizeReportsService service
            = new TestEntitySizeReportsService(connProvider, repo);
        return service.getEntitySizeEntities(
            entitySize, entityIdBound, boundType, pageSize, sampleSize);
    }
}
