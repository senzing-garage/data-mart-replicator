package com.senzing.datamart.reports;

import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.ProducesJson;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.Default;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.util.LoggingUtilities.formatStackTrace;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides services for the Entity Size reports.
 */
public interface EntitySizeReportsService extends ReportsService {
    /**
     * The prefix path for entity size reports services.
     */
    String ENTITY_SIZE_PREFIX = REPORTS_PREFIX + "/sizes";

    /**
     * The endpoint for the entity size breakdown report.
     */
    String ENTITY_SIZE_BREAKDOWN_ENDPOINT = ENTITY_SIZE_PREFIX + "/";

    /**
     * The endpoint for the entity size count report.
     */
    String ENTITY_SIZE_COUNT_ENDPOINT = ENTITY_SIZE_PREFIX + "/{entitySize}";

    /**
     * The endpoint for the entities report based on entity size.
     */
    String ENTITY_SIZE_ENTITIES_ENDPOINT = ENTITY_SIZE_COUNT_ENDPOINT + "/entities";

    /**
     * Exposes {@link EntitySizeReports#getEntitySizeBreakdown(Connection, Timers)}
     * as a REST/JSON service at {@link #ENTITY_SIZE_BREAKDOWN_ENDPOINT}.
     * 
     * @return The {@link SzEntitySizeBreakdown} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(ENTITY_SIZE_PREFIX)
    @Path(ENTITY_SIZE_BREAKDOWN_ENDPOINT)
    @ProducesJson
    default SzEntitySizeBreakdown getEntitySizeBreakdown() throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntitySizeReports.getEntitySizeBreakdown(conn, timers);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ReportsServiceException(e);

        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw e;

        } finally {
            conn = close(conn);
        }
    }

    /**
     * Exposes {@link EntitySizeReports#getEntitySizeCount(Connection, int, Timers)}
     * as a REST/JSON service at {@link #ENTITY_SIZE_COUNT_ENDPOINT}.
     * 
     * @param entitySize The entity size (record count) for which the entity count
     *                   is being requested.
     * 
     * @return The {@link SzEntitySizeCount} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(ENTITY_SIZE_COUNT_ENDPOINT)
    @ProducesJson
    default SzEntitySizeCount getEntitySizeCount(@Param("entitySize") int entitySize) throws ReportsServiceException {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntitySizeReports.getEntitySizeCount(conn, entitySize, timers);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ReportsServiceException(e);

        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw e;

        } finally {
            conn = close(conn);
        }

    }

    /**
     * Exposes
     * {@link EntitySizeReports#getEntityIdsForEntitySize(Connection, int, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at {@link #ENTITY_SIZE_ENTITIES_ENDPOINT}.
     * 
     * @param entitySize    The entity size (record count) for which the entity
     *                      count is being requested.
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
     * @return The {@link SzEntitySizeCount} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get
    @Path(ENTITY_SIZE_ENTITIES_ENDPOINT)
    @Path(ENTITY_SIZE_ENTITIES_ENDPOINT + "/")
    @ProducesJson
    default SzEntitiesPage getEntitySizeEntities(
            @Param("entitySize")                            int         entitySize,
            @Param("bound") @Nullable                       String      entityIdBound,
            @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType,
            @Param("pageSize") @Nullable                    Integer     pageSize,
            @Param("sampleSize") @Nullable                  Integer     sampleSize)
        throws ReportsServiceException 
    {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntitySizeReports.getEntityIdsForEntitySize(conn, 
                                                               entitySize,
                                                               entityIdBound,
                                                               boundType,
                                                               pageSize,
                                                               sampleSize, 
                                                               timers);
            
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ReportsServiceException(e);

        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw e;

        } finally {
            conn = close(conn);
        }
    }
}
