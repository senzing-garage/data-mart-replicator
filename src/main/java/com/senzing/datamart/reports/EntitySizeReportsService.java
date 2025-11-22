package com.senzing.datamart.reports;

import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.Default;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.close;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides services for the Entity Size Breakdown report.
 */
public interface EntitySizeReportsService extends ReportsService {
    /**
     * The prefix path for entity size reports services.
     */
    String ENTITY_SIZE_PREFIX = "/statistics/sizes";

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
    String ENTITY_SIZE_ENTITIES_ENDPOINT = ENTITY_SIZE_PREFIX + "/{entitySize}/entities";
    
    /**
     * Exposes {@link EntitySizeReports#getEntitySizeBreakdown(Connection, Timers)}
     * as a REST/JSON service at {@link #ENTITY_SIZE_BREAKDOWN_ENDPOINT}.
     * 
     * @return The {@link SzEntitySizeBreakdown} describing the report.
     * 
     * @throws SQLException If a failure occurs.
     */
    @Get(ENTITY_SIZE_BREAKDOWN_ENDPOINT)
    default SzEntitySizeBreakdown getEntitySizeBreakdown() 
        throws SQLException
    {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntitySizeReports.getEntitySizeBreakdown(conn, timers);
            
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
     * @throws SQLException If a failure occurs.
     */
    @Get(ENTITY_SIZE_COUNT_ENDPOINT)
    default SzEntitySizeCount getEntitySizeCount(@Param("entitySize") int entitySize) 
        throws SQLException
    {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntitySizeReports.getEntitySizeCount(conn, entitySize, timers);
            
        } finally {
            conn = close(conn);
        }

    }

    /**
     * Exposes {@link EntitySizeReports#getEntityIdsForEntitySize(Connection, int, String, SzBoundType, Integer, Integer, Timers)}
     * as a REST/JSON service at {@link #ENTITY_SIZE_ENTITIES_ENDPOINT}.
     * 
     * @param entitySize The entity size (record count) for which the entity count
     *                   is being requested.
     * @param entityIdBound The bound value for the entity ID's that will be
     *                      returned.
     * @param boundType The {@link SzBoundType} that describes how to apply the
     *                  specified entity ID bound.
     * @param pageSize The maximum number of entity ID's to return.
     * 
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * 
     * @return The {@link SzEntitySizeCount} describing the report.
     * 
     * @throws SQLException If a failure occurs.
     */
    @Get(ENTITY_SIZE_ENTITIES_ENDPOINT)
    default SzEntitiesPage getEntitySizeEntities(
            @Param("entitySize")                            int         entitySize,
            @Param("bound") @Nullable                       String      entityIdBound,
            @Param("boundType") @Default("EXCLUSIVE_LOWER") SzBoundType boundType,
            @Param("pageSize") @Nullable                    Integer     pageSize,
            @Param("sampleSize") @Nullable                  Integer     sampleSize)
        throws SQLException
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
        } finally {
            conn = close(conn);
        }

    }

}
