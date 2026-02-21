package com.senzing.datamart.reports;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntityRelationsBreakdown;
import com.senzing.datamart.reports.model.SzEntityRelationsCount;
import com.senzing.sql.ConnectionProvider;

/**
 * Tests the {@link EntityRelationsReportsService} interface methods using
 * {@link TestEntityRelationsReportsService}.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class EntityRelationsReportsServiceTest extends EntityRelationsReportsTest {

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestEntityRelationsReportsService} to obtain the result.
     */
    @Override
    protected SzEntityRelationsBreakdown getEntityRelationsBreakdown(
        RepositoryType     repoType,
        ConnectionProvider connProvider)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestEntityRelationsReportsService service
            = new TestEntityRelationsReportsService(connProvider, repo);
        return service.getEntityRelationsBreakdown();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestEntityRelationsReportsService} to obtain the result.
     */
    @Override
    protected SzEntityRelationsCount getEntityRelationsCount(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        int                relationsCount)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestEntityRelationsReportsService service
            = new TestEntityRelationsReportsService(connProvider, repo);
        return service.getEntityRelationsCount(relationsCount);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use {@link TestEntityRelationsReportsService} to obtain the result.
     */
    @Override
    protected SzEntitiesPage getEntityIdsForRelationCount(
        RepositoryType     repoType,
        ConnectionProvider connProvider,
        int                relationsCount,
        String             entityIdBound,
        SzBoundType        boundType,
        Integer            pageSize,
        Integer            sampleSize)
        throws Exception
    {
        Repository repo = DataMartTestExtension.getRepository(repoType);
        TestEntityRelationsReportsService service
            = new TestEntityRelationsReportsService(connProvider, repo);
        return service.getEntityRelationsEntities(
            relationsCount, entityIdBound, boundType, pageSize, sampleSize);
    }
}
