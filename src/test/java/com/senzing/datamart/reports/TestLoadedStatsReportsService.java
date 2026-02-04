package com.senzing.datamart.reports;

import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.sql.ConnectionProvider;

/**
 * Concrete test implementation of {@link LoadedStatsReportsService}
 * that extends {@link AbstractTestReportsService}.
 */
public class TestLoadedStatsReportsService
    extends AbstractTestReportsService
    implements LoadedStatsReportsService
{
    /**
     * Constructs with the specified {@link ConnectionProvider} and
     * {@link Repository}.
     *
     * @param connectionProvider The {@link ConnectionProvider} to use for
     *                           obtaining database connections.
     * @param repository The {@link Repository} to use for obtaining
     *                   data source information.
     *
     * @throws NullPointerException If either parameter is {@code null}.
     */
    public TestLoadedStatsReportsService(ConnectionProvider  connectionProvider,
                                         Repository          repository)
    {
        super(connectionProvider, repository);
    }
}
