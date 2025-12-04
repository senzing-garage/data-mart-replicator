package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.util.Timers;
import com.senzing.datamart.model.SzReportKey;

import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.reports.ReportUtilities.*;

/**
 * Provides Entity Size Breakdown Report functionality.
 */
public final class EntitySizeReports {
    /**
     * Private default constructor.
     */
    private EntitySizeReports() {
        // do nothing
    }

    /**
     * Gets an {@link SzEntitySizeBreakdown} describing the 
     * entity size breakdown which is the number of entities in the
     * entity repository for each distinct entity size that exists
     * in the entity repository.
     * 
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzEntitySizeBreakdown} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntitySizeBreakdown getEntitySizeBreakdown(Connection   conn,
                                                               Timers       timers)
        throws NullPointerException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        
        // get the connection
        PreparedStatement ps = null;
        ResultSet rs = null;

        SzEntitySizeBreakdown result = new SzEntitySizeBreakdown();
        try {

            // get the total entity count
            queryingDatabase(timers, "selectEntitySizeBreakdown");
            try {
                // prepare the total entity count query
                ps = conn.prepareStatement("SELECT statistic, entity_count "
                                           + "FROM sz_dm_report WHERE report=?");

                // bind the report code
                ps.setString(1, ENTITY_SIZE_BREAKDOWN.getCode());

                // execute the query
                rs = ps.executeQuery();

                // read the results
                while (rs.next()) {
                    SzEntitySizeCount sizeCount = new SzEntitySizeCount();
                    String stat = rs.getString(1);
                    long count = rs.getLong(2);
                    int size = Integer.parseInt(stat);

                    sizeCount.setEntitySize(size);
                    sizeCount.setEntityCount(count);

                    result.addEntitySizeCount(sizeCount);
                }

            } finally {
                queriedDatabase(timers, "selectEntitySizeBreakdown");
            }

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets the {@link SzEntitySizeCount} describing the number of entities 
     * in the entity repository having the specified entity size.
     * 
     * @param conn       The JDBC {@link Connection} to use.
     * @param entitySize The entity size for which the count is being requested.
     * @param timers     The optional {@link Timers} to track timing of the
     *                   operation.
     * 
     * @return The {@link SzEntitySizeCount} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified entity size is not
     *                                  positive.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntitySizeCount getEntitySizeCount(Connection   conn, 
                                                       int          entitySize,
                                                       Timers       timers) 
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        
        // get the connection
        PreparedStatement ps = null;
        ResultSet rs = null;
        SzEntitySizeCount result = new SzEntitySizeCount();

        result.setEntitySize(entitySize);
        try {
            queryingDatabase(timers, "selectCountForEntitySize");
            try {
                // prepare the statement
                ps = conn.prepareStatement("SELECT entity_count FROM sz_dm_report " 
                    + "WHERE report=? AND statistic=?");

                // bind the parameters
                ps.setString(1, ENTITY_SIZE_BREAKDOWN.getCode());
                ps.setString(2, String.valueOf(entitySize));

                // execute the query
                rs = ps.executeQuery();

                // check if we have a result
                if (rs.next()) {
                    long entityCount = rs.getLong(1);

                    result.setEntityCount(entityCount);
                } else {
                    result.setEntityCount(0L);
                }

            } finally {
                queriedDatabase(timers, "selectCountForEntitySize");
            }

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Retrieves a page of entity ID's that identifies entities having
     * the specified entity size.
     *
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param entitySize    The entity size for which the entity count is being
     *                      requested.
     * @param entityIdBound The bounded value for the returned entity ID's,
     *                      formatted as an integer or the word <code>"max"</code>.
     * @param boundType     The {@link SzBoundType} that describes how to apply the
     *                      specified entity ID bound.
     * @param pageSize      The maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers        The optional {@link Timers} to track timing of the
     *                      operation.
     * 
     * @return The {@link SzEntitiesPage} containing the entity IDs.
     * 
     * @throws NullPointerException If a required parameter is specified as
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified entity size is not
     *                                  positive, if the specified page size
     *                                  or sample size is less than one (1), 
     *                                  or if the sample size is specified and is
     *                                  greater-than or equal to the sample size.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntitiesPage getEntityIdsForEntitySize(Connection   conn, 
                                                           int          entitySize,
                                                           String       entityIdBound,
                                                           SzBoundType  boundType, 
                                                           Integer      pageSize,
                                                           Integer      sampleSize,
                                                           Timers       timers)
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        
        // check the entity size
        if (entitySize < 1) {
            throw new IllegalArgumentException(
                "The entity size cannot be less than one: " + entitySize);
        }

        SzReportKey reportKey = new SzReportKey(ENTITY_SIZE_BREAKDOWN, entitySize);

        return retrieveEntitiesPage(
            conn, reportKey, entityIdBound, boundType, pageSize, sampleSize, timers);

    }

}
