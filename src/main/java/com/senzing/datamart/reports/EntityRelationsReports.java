package com.senzing.datamart.reports;

import static com.senzing.sql.SQLUtilities.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntityRelationsBreakdown;
import com.senzing.datamart.reports.model.SzEntityRelationsCount;
import com.senzing.util.Timers;

import static com.senzing.datamart.model.SzReportCode.*;
import static com.senzing.datamart.reports.ReportUtilities.*;

/**
 * Provides Entity Relations Breakdown Report functionality.
 */
public final class EntityRelationsReports {
    /**
     * Private default constructor.
     */
    private EntityRelationsReports() {
        // do nothing
    }

    /**
     * Gets an {@link SzEntityRelationsBreakdown} describing the entity
     * relations breakdown which is the number of entities in the entity
     * repository for each distinct number of relations that exists
     * for an entity in the repository.
     * 
     * @param conn The non-null JDBC {@link Connection} to use.
     * 
     * @param timers The optional {@link Timers} to track timing of the operation.
     * 
     * @return The {@link SzEntityRelationsBreakdown} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntityRelationsBreakdown getEntityRelationsBreakdown(
            Connection  conn, 
            Timers      timers) 
        throws NullPointerException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");

        // get the connection
        PreparedStatement   ps = null;
        ResultSet           rs = null;

        SzEntityRelationsBreakdown result = new SzEntityRelationsBreakdown();
        try {
            queryingDatabase(timers, "selectEntityRelationsBreakdown");
            try {
                // get the connection to the data mart database
                ps = conn.prepareStatement(
                    "SELECT statistic, entity_count FROM sz_dm_report WHERE report=?");

                // bind the report code
                ps.setString(1, ENTITY_RELATION_BREAKDOWN.getCode());

                // execute the query
                rs = ps.executeQuery();
                
                // read the results
                while (rs.next()) {
                    SzEntityRelationsCount relationsCount = new SzEntityRelationsCount();
                    String stat = rs.getString(1);
                    long count = rs.getLong(2);
                    int relations = Integer.parseInt(stat);

                    relationsCount.setRelationsCount(relations);
                    relationsCount.setEntityCount(count);

                    result.addEntityRelationsCount(relationsCount);
                }

            } finally {
                queriedDatabase(timers, "selectEntityRelationsBreakdown");
            }

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets the {@link SzEntityRelationsCount} describing the number of
     * entities in the entity repository having the specified entity size.
     * 
     * @param conn The JDBC {@link Connection} to use.
     * 
     * @param relationsCount The number of relationships for the entities
     *                       that are being counted.
     * 
     * @param timers The optional {@link Timers} to track timing of the
     *               operation.
     * 
     * @return The {@link SzEntityRelationsCount} describing the statistics.
     * 
     * @throws NullPointerException If the specified {@link Connection} is
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified entity size is not
     *                                  positive.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntityRelationsCount getEntityRelationsCount(
            Connection  conn,
            int         relationsCount,
            Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException
    {
        // get the connection
        PreparedStatement ps = null;
        ResultSet rs = null;
        SzEntityRelationsCount result = new SzEntityRelationsCount();

        result.setRelationsCount(relationsCount);
        try {
            // get the connection to the data mart database
            queryingDatabase(timers, "selectCountForEntityRelations");
            try {
                // prepare the statement
                ps = conn.prepareStatement(
                        "SELECT entity_count FROM sz_dm_report " 
                        + "WHERE report=? AND statistic=?");

                // bind the parameters
                ps.setString(1, ENTITY_RELATION_BREAKDOWN.getCode());
                ps.setString(2, String.valueOf(relationsCount));

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
                queriedDatabase(timers, "selectCountForEntityRelations");
            }

            // return the result
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Retrieves a page of entity ID's that identifies entities having the
     * specified number of entity relations.
     *
     * @param conn           The non-null JDBC {@link Connection} to use.
     * @param relationsCount The number of entity relations for which the entity
     *                       count is being requested.
     * @param entityIdBound  The bounded value for the returned entity ID's,
     *                       formatted as an integer or the word <code>"max"</code>.
     * @param boundType      The {@link SzBoundType} that describes how to apply the
     *                       specified entity ID bound.
     * @param pageSize       The maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers         The optional {@link Timers} to track timing of the
     *                       operation.
     * 
     * @return The {@link SzEntitiesPage} containing the entity IDs.
     * 
     * @throws NullPointerException If a required parameter is specified as
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified relations count is
     *                                  negative, if the specified page size
     *                                  or sample size is less than one (1), 
     *                                  or if the sample size is specified and is
     *                                  greater-than or equal to the sample size.
     * 
     * @throws SQLException             If a JDBC failure occurs.
     */
    public static SzEntitiesPage getEntityIdsForRelationCount(Connection    conn,
                                                              int           relationsCount, 
                                                              String        entityIdBound, 
                                                              SzBoundType   boundType,
                                                              Integer       pageSize, 
                                                              Integer       sampleSize, 
                                                              Timers        timers)
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");

        // check the entity size
        if (relationsCount < 0) {
            throw new IllegalArgumentException(
                    "The relations count cannot be less than zero: " + relationsCount);
        }

        SzReportKey reportKey = new SzReportKey(ENTITY_RELATION_BREAKDOWN, relationsCount);

        return retrieveEntitiesPage(
            conn, reportKey, entityIdBound, boundType, pageSize, sampleSize, timers);
    }

}
