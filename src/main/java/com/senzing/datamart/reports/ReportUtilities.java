package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.senzing.datamart.model.SzReportKey;
import com.senzing.datamart.reports.model.SzReportEntity;
import com.senzing.datamart.reports.model.SzReportRecord;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzReportRelation;
import com.senzing.datamart.reports.model.SzRelationType;
import com.senzing.datamart.reports.model.SzRelationsPage;
import com.senzing.sql.DatabaseType;
import com.senzing.util.Timers;

import static com.senzing.datamart.reports.model.SzBoundType.*;
import static com.senzing.sql.SQLUtilities.*;

/**
 * Provides reporting utility functions and constants.
 */
public final class ReportUtilities {
    /**
     * Private default constructor.
     */
    private ReportUtilities() {
        // do nothing
    }

    /**
     * The default page size if no sample size is specified.
     */
    public static final int DEFAULT_PAGE_SIZE = 100;

    /**
     * The multiplier to calculate the default page size from the sample size when
     * the sample size is specified, but the page size is not specified.
     */
    public static final int SAMPLE_SIZE_MULTIPLIER = 20;

    /**
     * The standardized {@link Timers} key used for SQL queries.
     */
    public static final String DATABASE_QUERY_TIMING = "sqlQuery";

    /**
     * Transitions the specified {@link Timers} into the
     * {@link #DATABASE_QUERY_TIMING} stage.
     *
     * @param timers           The {@link Timers} instance to transition.
     * @param queryDescription A description of the query being executed.
     */
    public static void queryingDatabase(Timers timers, String queryDescription) {
        if (timers == null) {
            return;
        }
        timers.start(DATABASE_QUERY_TIMING, 
                     DATABASE_QUERY_TIMING + ":" + queryDescription);
    }

    /**
     * Concludes the {@link #DATABASE_QUERY_TIMING} stage for the specified
     * {@link Timers}.
     *
     * @param timers           The {@link Timers} instance to transition.
     * @param queryDescription A description of the query being executed.
     */
    public static void queriedDatabase(Timers timers, String queryDescription) {
        if (timers == null) {
            return;
        }
        timers.pause(DATABASE_QUERY_TIMING, 
                     DATABASE_QUERY_TIMING + ":" + queryDescription);
    }

    /**
     * Retrieves a page of entity ID's for a specific report key with the specified
     * bound applied.
     * 
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param reportKey     The non-null {@link SzReportKey} identifying the report
     *                      with which the entity ID's are associated.
     * @param entityIdBound The bounded value for the returned entity ID's,
     *                      formatted as an integer or the word <code>"max"</code>.
     * @param boundType     The {@link SzBoundType} describing how the entity ID
     *                      bound value is applied in retrieving the page.
     * @param pageSize      The optional maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers        The optional {@link Timers} to use.
     * 
     * @return The {@link SzEntitiesPage} describing the entities on the page.
     * 
     * @throws NullPointerException If a required parameter is specified as 
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified page size or sample
     *                                  size is less than one (1), or if the
     *                                  sample size is specified and is
     *                                  greater-than or equal to the sample size.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzEntitiesPage retrieveEntitiesPage(Connection    conn,
                                                      SzReportKey   reportKey,
                                                      String        entityIdBound,
                                                      SzBoundType   boundType,
                                                      Integer       pageSize,
                                                      Integer       sampleSize,
                                                      Timers        timers)
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        Objects.requireNonNull(reportKey, "Report key cannot be null");
        
        // check the request parameters
        if (pageSize != null && pageSize < 1) {
            throw new IllegalArgumentException(
                "If specified, the page size must be a positive integer: "
                + pageSize);
        }
        if (sampleSize != null && sampleSize < 1) {
            throw new IllegalArgumentException(
                    "If specified, the sample size must be a positive integer: "
                    + sampleSize);
        }
        if (pageSize != null && sampleSize != null && sampleSize >= pageSize) {
            throw new IllegalArgumentException(
                    "If both the page size and sample size are specified then "
                    + "the sample size (" + sampleSize + ") must be strictly "
                    + "less-than the page size (" + pageSize + ")");
        }

        // default the page size if not specified
        if (pageSize == null) {
            pageSize = (sampleSize == null) ? DEFAULT_PAGE_SIZE 
                : SAMPLE_SIZE_MULTIPLIER * sampleSize;
        }

        // create the results list
        List<SzReportEntity> pageResults = new ArrayList<>(pageSize);

        // check if the bound type is null (this should not be the case)
        if (boundType == null) {
            boundType = ("max".equals(entityIdBound)) ? EXCLUSIVE_UPPER : EXCLUSIVE_LOWER;
        }

        String formattedReportKey = reportKey.toString();

        long boundValue = 0L;
        // default the bound if not specified
        if (entityIdBound == null) {
            boundValue = (boundType.isLower()) ? 0L : Long.MAX_VALUE;
            entityIdBound = (boundType.isLower()) ? "0" : "max";

        } else if ("max".equals(entityIdBound.trim().toLowerCase())) {
            boundValue = Long.MAX_VALUE;
            entityIdBound = entityIdBound.trim().toLowerCase();

        } else {
            try {
                boundValue = Long.parseLong(entityIdBound.trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "The entity ID bound must either be an integer or \"max\": "
                    + entityIdBound);
            }
        }

        // track the min and max tracking and result count
        long minEntityId = -1L;
        long maxEntityId = -1L;
        int resultCount = 0;

        // prepare the result object the rest of the page
        SzEntitiesPage page = new SzEntitiesPage();
        page.setBound(entityIdBound);
        page.setBoundType(boundType);
        page.setPageSize(pageSize);
        page.setSampleSize(sampleSize);

        // setup the JDBC variables
        PreparedStatement ps = null;
        ResultSet rs = null;
        queryingDatabase(timers, "selectPagedEntities");
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT t1.entity_id, t2.entity_name," 
                    + " t2.record_count, t2.relation_count,"
                    + " t3.data_source, t3.record_id,"
                    + " t3.match_key, t3.errule_code "
                    + "FROM sz_dm_report_detail t1 "
                    + "LEFT OUTER JOIN sz_dm_entity t2 "
                    + "ON t1.entity_id = t2.entity_id "
                    + "LEFT OUTER JOIN sz_dm_record t3 "
                    + "ON t1.entity_id = t3.entity_id "
                    + "WHERE t1.entity_id IN ( ");

            sb.append("SELECT entity_id FROM sz_dm_report_detail "
                    + "WHERE report_key = ? AND related_id = 0 AND entity_id ");

            // handle the operator and order-by for the bound type
            switch (boundType) {
            case INCLUSIVE_LOWER:
                sb.append(" >= ? ORDER BY entity_id ASC ");
                break;
            case EXCLUSIVE_LOWER:
                sb.append(" > ? ORDER BY entity_id ASC ");
                break;
            case INCLUSIVE_UPPER:
                sb.append(" <= ? ORDER BY entity_id DESC ");
                break;
            case EXCLUSIVE_UPPER:
                sb.append(" < ? ORDER BY entity_id DESC ");
                break;
            default:
                throw new IllegalStateException("Unhandled bound type: " + boundType);
            }

            // handle the page size
            sb.append("LIMIT ?");
            sb.append(") ORDER BY t1.entity_id, t3.data_source, t3.record_id");

            // prepare the statement
            ps = conn.prepareStatement(sb.toString());

            // bind the parameters
            ps.setString(1, formattedReportKey);
            ps.setLong(2, boundValue);
            ps.setInt(3, pageSize);

            // execute the query
            rs = ps.executeQuery();

            // read the results until we have read enough for the page
            SzReportEntity entity = null;
            while (rs.next()) {
                // get the fields
                long entityId = rs.getLong(1);
                String entityName = getString(rs, 2);
                Integer recordCount = getInt(rs, 3);
                Integer relationCount = getInt(rs, 4);
                String dataSource = getString(rs, 5);
                String recordId = getString(rs, 6);
                String matchKey = getString(rs, 7);
                String principle = getString(rs, 8);

                // create the record object
                SzReportRecord record = null;
                if (dataSource != null && recordId != null) {
                    record = new SzReportRecord(dataSource, recordId);
                    record.setMatchKey(matchKey);
                    record.setPrinciple(principle);
                }

                // check if we need to finish with the entity
                if (entity != null && entity.getEntityId() != entityId) {
                    // add to the page results
                    pageResults.add(entity);

                    // track the minimum and maximum entity ID
                    if (minEntityId < 0L || entity.getEntityId() < minEntityId) 
                    {
                        minEntityId = entity.getEntityId();
                    }
                    if (maxEntityId < 0L || entity.getEntityId() > maxEntityId) 
                    {
                        maxEntityId = entity.getEntityId();
                    }

                    // set the entity to null
                    entity = null;

                    // break out if we hit the maximum number of results
                    if (pageResults.size() >= pageSize) {
                        break;
                    }
                }

                // check if the entity is null
                if (entity == null) {
                    // create a new entity
                    entity = new SzReportEntity(entityId, entityName);
                    entity.setRecordCount(recordCount);
                    entity.setRelationCount(relationCount);
                }

                // add the record to the entity
                if (record != null) {
                    entity.addRecord(record);
                }
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // handle the last entity
            if (entity != null) {
                // add to the page results
                pageResults.add(entity);

                // track the minimum and maximum entity ID
                if (minEntityId < 0L || entity.getEntityId() < minEntityId) {
                    minEntityId = entity.getEntityId();
                }
                if (maxEntityId < 0L || entity.getEntityId() > maxEntityId) {
                    maxEntityId = entity.getEntityId();
                }
            }

            // get the actual count of results on the page
            resultCount = pageResults.size();

            // now filter the results if we have a sample size
            if (sampleSize != null && pageResults.size() > sampleSize) {
                // set the page minimum and maximum value
                page.setPageMinimumValue(minEntityId);
                page.setPageMaximumValue(maxEntityId);

                // create a list of indices
                List<Integer> indices = new ArrayList<>();
                for (int index = 0; index < resultCount; index++) {
                    indices.add(index);
                }

                // shuffle the list of indices
                Collections.shuffle(indices);

                // determine how many results to filter out
                int filterCount = resultCount - sampleSize;

                // now eliminate the filtered results using first N indices
                for (int index = 0; index < filterCount; index++) {
                    int resultIndex = indices.get(index);
                    pageResults.set(resultIndex, null);
                }
            }

            // add the non-filtered results to the page
            pageResults.forEach(resultEntity -> {
                // check if the result was filtered out from random sampling
                if (resultEntity != null) {
                    // the result was not filtered so add it to the page
                    page.addEntity(resultEntity);
                }
            });

        } finally {
            queriedDatabase(timers, "selectPagedEntities");
        }

        // now get the total entity ID count
        long totalCount = 0L;
        queryingDatabase(timers, "selectTotalEntityPageCount");
        try {
            // prepare the query
            ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM sz_dm_report_detail " 
                    + "WHERE report_key = ? AND related_id = 0");
            ps.setString(1, formattedReportKey);
            rs = ps.executeQuery();
            rs.next();
            totalCount = rs.getLong(1);
            rs = close(rs);
            ps = close(ps);

        } finally {
            queriedDatabase(timers, "selectTotalEntityPageCount");
            rs = close(rs);
            ps = close(ps);
        }

        // now get the "before" and "after" entity ID count
        long beforeCount = 0L;
        long afterCount = 0L;
        queryingDatabase(timers, "selectBeforePageEntityCount");
        try {
            if (resultCount > 0) {
                ps = conn.prepareStatement("SELECT COUNT(*) FROM sz_dm_report_detail "
                        + "WHERE report_key = ? AND entity_id < ? AND related_id = 0");

                ps.setString(1, formattedReportKey);
                ps.setLong(2, minEntityId);

                rs = ps.executeQuery();
                rs.next();

                beforeCount = rs.getLong(1);

                rs = close(rs);
                ps = close(ps);
            }
            // calculate the "after" count from total, page and before count
            afterCount = totalCount - resultCount - beforeCount;

            // set the fields on the page
            page.setTotalEntityCount(totalCount);
            page.setBeforePageCount(beforeCount);
            page.setAfterPageCount(afterCount);

            // return the page
            return page;

        } finally {
            queriedDatabase(timers, "selectBeforePageEntityCount");
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Retrieves a page of relations for a specific report key with the specified
     * bound applied.
     * 
     * @param conn          The non-null JDBC {@link Connection} to use.
     * @param reportKey     The non-null {@link SzReportKey} identifying the report
     *                      with which the entity ID's are associated.
     * @param relationBound The bounded value for the returned relations.
     * @param boundType     The {@link SzBoundType} describing how the entity ID
     *                      bound value is applied in retrieving the page.
     * @param pageSize      The optional maximum number of entity ID's to return.
     * @param sampleSize    The optional number of results to randomly sample from
     *                      the page, which, if specified, must be strictly
     *                      less-than the page size.
     * @param timers        The optional {@link Timers} to use.
     * 
     * @return The {@link SzRelationsPage} describing the relations on the page.
     * 
     * @throws NullPointerException If a required parameter is specified as 
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified page size or sample
     *                                  size is less than one (1), or if the
     *                                  sample size is specified and is
     *                                  greater-than or equal to the sample size.
     * 
     * @throws SQLException If a JDBC failure occurs.
     */
    public static SzRelationsPage retrieveRelationsPage(Connection  conn,
                                                        SzReportKey reportKey,
                                                        String      relationBound,
                                                        SzBoundType boundType,
                                                        Integer     pageSize,
                                                        Integer     sampleSize,
                                                        Timers      timers)
        throws NullPointerException, IllegalArgumentException, SQLException 
    {
        Objects.requireNonNull(conn, "The connection cannot be null");
        Objects.requireNonNull(reportKey, "Report key cannot be null");
        
        // check the request parameters
        if (pageSize != null && pageSize < 1) {
            throw new IllegalArgumentException(
                    "If specified, the page size must be a positive integer: "
                    + pageSize);
        }
        if (sampleSize != null && sampleSize < 1) {
            throw new IllegalArgumentException(
                    "If specified, the sample size must be a positive integer: "
                    + sampleSize);
        }
        if (pageSize != null && sampleSize != null && sampleSize >= pageSize) {
            throw new IllegalArgumentException(
                    "If both the page size and sample size are specified then the "
                    + "sample size (" + sampleSize + ") must be strictly less-than "
                    + "the page size (" + pageSize + ")");
        }

        // check if the bound type is null (this should not be the case)
        if (boundType == null) {
            boundType = ("max:max".equals(relationBound)) 
                ? EXCLUSIVE_UPPER : EXCLUSIVE_LOWER;
        }

        String formattedReportKey = reportKey.toString();

        // parse the relation bound
        long defaultValue = (boundType.isLower()) ? 0L : Long.MAX_VALUE;
        long entityIdBound = defaultValue;
        long relatedIdBound = defaultValue;
        if (relationBound != null) {
            relationBound = relationBound.trim();
            if (relationBound.length() > 0) {
                try {
                    int index = relationBound.indexOf(":");
                    if (index < 0) {
                        if ("max".equals(relationBound.toLowerCase().trim())) {
                            entityIdBound = Long.MAX_VALUE;
                            relatedIdBound = Long.MAX_VALUE;
                            relationBound = "max:max";
                        } else {
                            entityIdBound = Long.parseLong(relationBound);
                        }
                    
                    } else if (index == 0) {
                        throw new IllegalArgumentException();

                    } else if (index > 0) {
                        String part1 = relationBound.substring(0, index).trim();
                        if ("max".equals(part1.toLowerCase())) {
                            entityIdBound = Long.MAX_VALUE;
                            part1 = "max";
                        } else {
                            entityIdBound = Long.parseLong(part1);
                        }
                        String part2 = (index == relationBound.length() - 1) ? ""
                                : relationBound.substring(index + 1).trim();
                        if ("max".equals(part2.toLowerCase())) {
                            relatedIdBound = Long.MAX_VALUE;
                            part2 = "max";
                        } else {
                            relatedIdBound = (part2.length() == 0) 
                                ? defaultValue : Long.parseLong(part2);
                        }
                        relationBound = part1 + ":" + part2;
                    }

                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "The specified relation bound is not properly formatted: "
                            + relationBound);
                }
            }
        }

        if (relationBound == null) {
            relationBound = (boundType.isLower()) ? "0:0" : "max:max";
        }

        // default the page size if not specified
        if (pageSize == null) {
            pageSize = (sampleSize == null) ? DEFAULT_PAGE_SIZE
                : SAMPLE_SIZE_MULTIPLIER * sampleSize;
        }

        // create
        List<SzReportRelation> pageResults = new ArrayList<>(pageSize);

        // initialize min and max tracking and result count
        long minEntityId = -1L;
        long minRelatedId = -1L;
        long maxEntityId = -1L;
        long maxRelatedId = -1L;
        int resultCount = 0;

        // prepare the result object the rest of the page
        SzRelationsPage page = new SzRelationsPage();
        page.setBound(relationBound);
        page.setBoundType(boundType);
        page.setPageSize(pageSize);
        page.setSampleSize(sampleSize);

        // setup the JDBC variables
        PreparedStatement ps = null;
        ResultSet rs = null;
        queryingDatabase(timers, "selectPagedRelations");
        try {
            // get the database type
            DatabaseType dbType = DatabaseType.detect(conn);

            StringBuilder sb = new StringBuilder();
            sb.append("SELECT entity_id, related_id FROM sz_dm_report_detail "
                    + "WHERE report_key = ? AND related_id <> 0 AND ");

            // handle the operator and order-by for the bound type
            switch (boundType) {
            case INCLUSIVE_LOWER:
                sb.append("((entity_id = ? AND related_id >= ?)");
                sb.append(" OR (entity_id > ?)) ");
                sb.append("ORDER BY entity_id ASC, related_id ASC ");
                break;
            case EXCLUSIVE_LOWER:
                sb.append("((entity_id = ? AND related_id > ?)");
                sb.append(" OR (entity_id > ?)) ");
                sb.append("ORDER BY entity_id ASC, related_id ASC ");
                break;
            case INCLUSIVE_UPPER:
                sb.append("((entity_id = ? AND related_id <= ?)");
                sb.append(" OR (entity_id < ?)) ");
                sb.append("ORDER BY entity_id DESC, related_id DESC ");
                break;
            case EXCLUSIVE_UPPER:
                sb.append("((entity_id = ? AND related_id < ?)");
                sb.append(" OR (entity_id < ?)) ");
                sb.append("ORDER BY entity_id DESC, related_id DESC ");
                break;
            default:
                throw new IllegalStateException("Unhandled bound type: " + boundType);
            }

            // handle the page size
            sb.append("LIMIT ?");
            String innerQuery = sb.toString();
            sb.delete(0, sb.length());

            // now do the outer query
            sb.append("SELECT rel_entity_id, rel_related_id," 
                    + " match_type, rel_match_key, rel_rev_match_key, rel_errule_code,"
                    + " entity_id, entity_name, record_count, relation_count,"
                    + " data_source, record_id, match_key, errule_code " 
                    + "FROM (SELECT"
                    + " t1.entity_id AS rel_entity_id," 
                    + " t1.related_id AS rel_related_id,"
                    + " t2.match_type AS match_type," 
                    + " t2.match_key AS rel_match_key,"
                    + " t2.rev_match_key AS rel_rev_match_key,"
                    + " t2.errule_code AS rel_errule_code,"
                    + " t3.entity_id AS entity_id, t3.entity_name AS entity_name," 
                    + " t3.record_count AS record_count,"
                    + " t3.relation_count AS relation_count,"
                    + " t4.data_source AS data_source, t4.record_id AS record_id,"
                    + " t4.match_key AS match_key, t4.errule_code AS errule_code " 
                    + "FROM sz_dm_report_detail AS t1 "
                    + "LEFT OUTER JOIN sz_dm_relation AS t2 "
                    + "ON t2.entity_id = "
                    + dbType.sqlLeast("t1.entity_id", "t1.related_id") 
                    + " AND t2.related_id = "
                    + dbType.sqlGreatest("t1.entity_id", "t1.related_id") 
                    + " LEFT OUTER JOIN sz_dm_entity AS t3 "
                    + "ON t1.entity_id = t3.entity_id " 
                    + "LEFT OUTER JOIN sz_dm_record AS t4 "
                    + "ON t1.entity_id = t4.entity_id " 
                    + "WHERE (t1.entity_id, t1.related_id) IN (" + innerQuery + ") "
                    + "UNION SELECT" 
                    + " t5.entity_id AS rel_entity_id,"
                    + " t5.related_id AS rel_related_id,"
                    + " t6.match_type AS match_type,"
                    + " t6.match_key AS rel_match_key,"
                    + " t6.rev_match_key AS rel_rev_match_key,"
                    + " t6.errule_code AS rel_errule_code,"
                    + " t7.entity_id AS entity_id, t7.entity_name AS entity_name,"
                    + " t7.record_count AS record_count,"
                    + " t7.relation_count AS relation_count,"
                    + " t8.data_source AS data_source, t8.record_id AS record_id,"
                    + " t8.match_key AS match_key, t8.errule_code AS errule_code "
                    + "FROM sz_dm_report_detail AS t5 "
                    + "LEFT OUTER JOIN sz_dm_relation AS t6 " 
                    + "ON t6.entity_id = "
                    + dbType.sqlLeast("t5.entity_id", "t5.related_id")
                    + " AND t6.related_id = "
                    + dbType.sqlGreatest("t5.entity_id", "t5.related_id")
                    + " LEFT OUTER JOIN sz_dm_entity AS t7 "
                    + "ON t5.related_id = t7.entity_id "
                    + "LEFT OUTER JOIN sz_dm_record AS t8 "
                    + "ON t5.related_id = t8.entity_id "
                    + "WHERE (t5.entity_id, t5.related_id) IN (" + innerQuery + ") " 
                    + ") AS relations_page " 
                    + "ORDER BY rel_entity_id, rel_related_id, entity_id,"
                    + " data_source, record_id");

            // prepare the statement
            ps = conn.prepareStatement(sb.toString());

            // bind the parameters
            ps.setString(1, formattedReportKey);
            ps.setLong(2, entityIdBound);
            ps.setLong(3, relatedIdBound);
            ps.setLong(4, entityIdBound);
            ps.setInt(5, pageSize);
            ps.setString(6, formattedReportKey);
            ps.setLong(7, entityIdBound);
            ps.setLong(8, relatedIdBound);
            ps.setLong(9, entityIdBound);
            ps.setInt(10, pageSize);

            // execute the query
            rs = ps.executeQuery();

            // read the results until we have read enough for the page
            SzReportRelation relation = null;
            while (rs.next()) {
                long relEntityId = rs.getLong(1);
                long relRelatedId = rs.getLong(2);
                String relTypeText = getString(rs, 3);
                String relMatchKey = getString(rs, 4);
                String relRevMatchKey = getString(rs, 5);
                String relPrinciple = getString(rs, 6);

                Long entityId = getLong(rs, 7);
                String entityName = getString(rs, 8);
                int recordCount = rs.getInt(9);
                int relationCount = rs.getInt(10);
                String dataSource = getString(rs, 11);
                String recordId = getString(rs, 12);
                String matchKey = getString(rs, 13);
                String principle = getString(rs, 14);

                SzRelationType relationType = (relTypeText == null) 
                    ? null : SzRelationType.valueOf(relTypeText);

                // create the record object
                SzReportRecord record = null;
                if (dataSource != null && recordId != null) {
                    record = new SzReportRecord(dataSource, recordId);
                    record.setMatchKey(matchKey);
                    record.setPrinciple(principle);
                }

                // check if we need to finish with the relation
                if (relation != null 
                    && (relation.getEntity().getEntityId() != relEntityId
                        || relation.getRelatedEntity().getEntityId() != relRelatedId)) 
                {
                    // add to the page results
                    pageResults.add(relation);

                    SzReportEntity entity = relation.getEntity();
                    SzReportEntity related = relation.getRelatedEntity();

                    // track the minimums and maximums
                    if (minEntityId < 0L || entity.getEntityId() < minEntityId) 
                    {
                        minEntityId = entity.getEntityId();
                        minRelatedId = related.getEntityId();

                    } else if (entity.getEntityId() == minEntityId
                               && related.getEntityId() < minRelatedId) 
                    {
                        minRelatedId = related.getEntityId();
                    }

                    if (maxEntityId < 0L || entity.getEntityId() > maxEntityId) 
                    {
                        maxEntityId = entity.getEntityId();
                        maxRelatedId = related.getEntityId();

                    } else if (entity.getEntityId() == maxEntityId
                               && related.getEntityId() > maxRelatedId) 
                    {
                        maxRelatedId = related.getEntityId();
                    }

                    // set the relation to null
                    relation = null;

                    // break out if we hit the maximum number o results
                    if (pageResults.size() >= pageSize) {
                        break;
                    }
                }

                // check if the relation is null
                if (relation == null) {
                    // create a new entity, related entity and relation
                    SzReportEntity entity = new SzReportEntity(relEntityId);
                    SzReportEntity related = new SzReportEntity(relRelatedId);
                    relation = new SzReportRelation();
                    relation.setEntity(entity);
                    relation.setRelatedEntity(related);
                    relation.setRelationType(relationType);
                    relation.setPrinciple(relPrinciple);

                    // choose the reverse match key if the related entity ID 
                    // is less than the entity ID
                    relation.setMatchKey(
                        (relRelatedId < relEntityId) ? relRevMatchKey : relMatchKey);
                }

                // get the entity to be updated
                SzReportEntity targetEntity = null;
                if (entityId != null) {
                    targetEntity = (entityId.longValue() == relEntityId)
                        ? relation.getEntity()
                        : relation.getRelatedEntity();
                }

                // set the fields on the entity
                if (targetEntity != null) {
                    targetEntity.setEntityName(entityName);
                    targetEntity.setRecordCount(recordCount);
                    targetEntity.setRelationCount(relationCount);
                    if (record != null) {
                        targetEntity.addRecord(record);
                    }
                }
            }

            // release resources
            rs = close(rs);
            ps = close(ps);

            // handle the last relation
            if (relation != null) {
                SzReportEntity entity = relation.getEntity();
                SzReportEntity related = relation.getRelatedEntity();

                // add to the page results
                pageResults.add(relation);

                // track the minimums and maximums
                if (minEntityId < 0L || entity.getEntityId() < minEntityId) 
                {
                    minEntityId = entity.getEntityId();
                    minRelatedId = related.getEntityId();

                } else if (entity.getEntityId() == minEntityId
                           && related.getEntityId() < minRelatedId) 
                {
                    minRelatedId = related.getEntityId();
                }

                if (maxEntityId < 0L || entity.getEntityId() > maxEntityId) 
                {
                    maxEntityId = entity.getEntityId();
                    maxRelatedId = related.getEntityId();

                } else if (entity.getEntityId() == maxEntityId 
                           && related.getEntityId() > maxRelatedId) 
                {
                    maxRelatedId = related.getEntityId();
                }
            }

            // get the actual count of results on the page
            resultCount = pageResults.size();

            // now filter the results if we have a sample size
            if (sampleSize != null && pageResults.size() > sampleSize) {
                // set the page minimum and maximum values
                page.setPageMinimumValue(minEntityId + ":" + minRelatedId);
                page.setPageMaximumValue(maxEntityId + ":" + maxRelatedId);

                // create a list of indices
                List<Integer> indices = new ArrayList<>();
                for (int index = 0; index < resultCount; index++) {
                    indices.add(index);
                }

                // shuffle the list of indices
                Collections.shuffle(indices);

                // determine how many results to filter out
                int filterCount = resultCount - sampleSize;

                // now eliminate the filtered results using first N indices
                for (int index = 0; index < filterCount; index++) {
                    int resultIndex = indices.get(index);
                    pageResults.set(resultIndex, null);
                }
            }

            // add the non-filtered results to the page
            pageResults.forEach(resultRelation -> {
                // check if the result was filtered out from random sampling
                if (resultRelation != null) {
                    // the result was not filtered so add it to the page
                    page.addRelation(resultRelation);
                }
            });

        } finally {
            queriedDatabase(timers, "selectPagedRelations");
            rs = close(rs);
            ps = close(ps);
        }

        // now get the total relation count
        long totalCount = 0L;
        queryingDatabase(timers, "selectTotalRelationsPageCount");
        try {
            // prepare the statement
            ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM sz_dm_report_detail " + "WHERE report_key = ? AND related_id <> 0");
            ps.setString(1, formattedReportKey);
            rs = ps.executeQuery();
            rs.next();
            totalCount = rs.getLong(1);
            rs = close(rs);
            ps = close(ps);

        } finally {
            queriedDatabase(timers, "selectTotalRelationsPageCount");
            rs = close(rs);
            ps = close(ps);
        }

        // now get the "before" and "after" relation count
        long beforeCount = 0L;
        long afterCount = 0L;
        queryingDatabase(timers, "selectBeforePageRelationCount");
        try {
            if (resultCount > 0) {
                ps = conn.prepareStatement(
                        "SELECT COUNT(*) FROM sz_dm_report_detail " 
                        + "WHERE report_key = ? AND related_id <> 0 AND "
                        + "((entity_id = ? AND related_id < ?) OR (entity_id < ?))");

                ps.setString(1, formattedReportKey);
                ps.setLong(2, minEntityId);
                ps.setLong(3, minRelatedId);
                ps.setLong(4, minEntityId);

                rs = ps.executeQuery();
                rs.next();

                beforeCount = rs.getLong(1);

                rs = close(rs);
                ps = close(ps);
            }
            // calculate the "after" count from total, page and before count
            afterCount = totalCount - resultCount - beforeCount;

            // set the fields on the page
            page.setTotalRelationCount(totalCount);
            page.setBeforePageCount(beforeCount);
            page.setAfterPageCount(afterCount);

            // return the page
            return page;

        } finally {
            queriedDatabase(timers, "selectBeforePageRelationCount");
            rs = close(rs);
            ps = close(ps);
        }
    }

}
