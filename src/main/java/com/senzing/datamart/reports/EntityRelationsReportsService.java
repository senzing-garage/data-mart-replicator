package com.senzing.datamart.reports;

import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.ProducesJson;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.annotation.Default;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntityRelationsBreakdown;
import com.senzing.datamart.reports.model.SzEntityRelationsCount;
import com.senzing.util.Timers;

import static com.senzing.sql.SQLUtilities.close;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides services for the Entity Relations reports.
 */
public interface EntityRelationsReportsService extends ReportsService {
    /**
     * The prefix path for entity relations count reports services.
     */
    String ENTITY_RELATIONS_PREFIX = REPORTS_PREFIX + "/relations";

    /**
     * The endpoint for the entity relations breakdown report.
     */
    String ENTITY_RELATIONS_BREAKDOWN_ENDPOINT = ENTITY_RELATIONS_PREFIX + "/";

    /**
     * The endpoint for the entity relations count report.
     */
    String ENTITY_RELATIONS_COUNT_ENDPOINT 
        = ENTITY_RELATIONS_PREFIX + "/{relationCount}";

    /**
     * The endpoint for the entities report based on entity relations count.
     */
    String ENTITY_RELATIONS_ENTITIES_ENDPOINT 
        = ENTITY_RELATIONS_COUNT_ENDPOINT + "/entities";
    
    /**
     * Exposes {@link EntityRelationsReports#getEntityRelationsBreakdown(Connection, Timers)}
     * as a REST/JSON service at {@link #ENTITY_RELATIONS_BREAKDOWN_ENDPOINT}.
     * 
     * @return The {@link SzEntityRelationsBreakdown} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(ENTITY_RELATIONS_BREAKDOWN_ENDPOINT)
    @ProducesJson
    default SzEntityRelationsBreakdown getEntityRelationsBreakdown() 
        throws ReportsServiceException
    {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntityRelationsReports.getEntityRelationsBreakdown(conn, timers);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new ReportsServiceException(e);

        } catch (Exception e) { 
            e.printStackTrace();
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            } else {
                throw new ReportsServiceException(e);
            }

        } finally {
            conn = close(conn);
        }
    }

    /**
     * Exposes {@link EntityRelationsReports#getEntityRelationsCount(
     * Connection, int, Timers)} as a REST/JSON service at 
     * {@link #ENTITY_RELATIONS_COUNT_ENDPOINT}.
     * 
     * @param relationCount The number of relations for which the entity count
     *                      is being requested.
     * 
     * @return The {@link SzEntityRelationsCount} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(ENTITY_RELATIONS_COUNT_ENDPOINT)
    @ProducesJson
    default SzEntityRelationsCount getEntityRelationsCount(
            @Param("relationCount") int relationCount) 
        throws ReportsServiceException
    {
        Connection conn = null;
        try {
            conn = this.getConnection();

            Timers timers = this.getTimers();

            return EntityRelationsReports.getEntityRelationsCount(
                conn, relationCount, timers);
            
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ReportsServiceException(e);

        } catch (Exception e) { 
            e.printStackTrace();
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            } else {
                throw new ReportsServiceException(e);
            }
        
        } finally {
            conn = close(conn);
        }

    }

    /**
     * Exposes {@link EntityRelationsReports#getEntityIdsForRelationCount(Connection,
     * int, String, SzBoundType, Integer, Integer, Timers)} as a REST/JSON service at 
     * {@link #ENTITY_RELATIONS_ENTITIES_ENDPOINT}.
     * 
     * @param relationCount The number of relations for which the entity count
     *                      is being requested.
     * @param entityIdBound The bound value for the entity ID's that will be
     *                      returned.
     * @param boundType     The {@link SzBoundType} that describes how to apply 
     *                      the specified entity ID bound.
     * @param pageSize      The maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * 
     * @return The {@link SzEntitiesPage} describing the report.
     * 
     * @throws ReportsServiceException If a failure occurs.
     */
    @Get(ENTITY_RELATIONS_ENTITIES_ENDPOINT)
    @ProducesJson
    default SzEntitiesPage getEntityRelationsEntities(
            @Param("relationCount")                         int         relationCount,
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

            return EntityRelationsReports.getEntityIdsForRelationCount(conn, 
                                                                       relationCount,
                                                                       entityIdBound,
                                                                       boundType,
                                                                       pageSize,
                                                                       sampleSize,
                                                                       timers);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ReportsServiceException(e);

        } catch (Exception e) { 
            e.printStackTrace();
            if (e instanceof RuntimeException) {
                throw ((RuntimeException) e);
            } else {
                throw new ReportsServiceException(e);
            }

        } finally {
            conn = close(conn);
        }

    }

}
