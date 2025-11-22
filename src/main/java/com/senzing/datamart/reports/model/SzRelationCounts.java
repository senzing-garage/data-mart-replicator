package com.senzing.datamart.reports.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes the entity, record and relationship counts for the respective
 * relation type for entities having at least one record from the primary data
 * source to entities having at least one record from the "versus" data source.
 * The statistics are optionally associated with a specific match key or
 * principle.
 */
public class SzRelationCounts implements Serializable {
    /**
     * The optional match key associated with the statistics. This may be
     * <code>null</code> or absent if the statistics are not associated with a match
     * key.
     */
    private String matchKey = null;

    /**
     * The optional principle associated with the statistics. This may be
     * <code>null</code> or absent if the statistics are not associated with a
     * principle.
     */
    private String principle = null;

    /**
     * The number of entities associated with the statistic.
     */
    private long entityCount = 0L;

    /**
     * The number of records associated with the statistic.
     */
    private long recordCount = 0L;

    /**
     * The number of relations associated with the statistic.
     */
    private long relationCount = 0L;

    /**
     * Default constructor. This constructs an instance with no associated match key
     * or principle.
     */
    public SzRelationCounts() {
        this(null, null);
    }

    /**
     * Constructs with the optional match key and principle.
     * 
     * @param matchKey  The optionally associated match key, or <code>null</code> if
     *                  no specific match key is associated.
     * @param principle The optionally associated principle, or <code>null</code> if
     *                  no specific principle is associated.
     */
    public SzRelationCounts(String matchKey, String principle) {
        this.matchKey = matchKey;
        this.principle = principle;
        this.entityCount = 0L;
        this.recordCount = 0L;
        this.relationCount = 0L;
    }

    /**
     * Gets the optional match key associated with the statistics. This may be
     * <code>null</code> or absent if the statistics are not associated with a match
     * key.
     * 
     * @return The optional match key associated with the statistics, or
     *         <code>null</code> or absent if the statistics are not associated with
     *         a match key.
     */
    @JsonInclude(NON_NULL)
    public String getMatchKey() {
        return this.matchKey;
    }

    /**
     * Gets the optional principle associated with the statistics. This may be
     * <code>null</code> or absent if the statistics are not associated with a
     * principle.
     * 
     * @return The optional principle associated with the statistics, or
     *         <code>null</code> or absent if the statistics are not associated with
     *         a principle.
     */
    @JsonInclude(NON_NULL)
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * The number of entities having at least one record from the primary data
     * source related by a relationship of the respective relationship type to an
     * entity with at least one record from the "versus" data source.
     * 
     * @return The number of entities having at least one record from the primary
     *         data source related by a relationship of the respective relationship
     *         type to an entity with at least one record from the "versus" data
     *         source.
     */
    public long getEntityCount() {
        return this.entityCount;
    }

    /**
     * Sets the number of entities having at least one record from the primary data
     * source related by a relationship of the respective relationship type to an
     * entity with at least one record from the "versus" data source.
     * 
     * @param entityCount The number of entities having at least one record from the
     *                    primary data source related by a relationship of the
     *                    respective relationship type to an entity with at least
     *                    one record from the "versus" data source.
     */
    public void setEntityCount(long entityCount) {
        this.entityCount = entityCount;
    }

    /**
     * Gets the number of records from the primary data source in the entities
     * described by the {@linkplain #getEntityCount() entityCount}. <b>NOTE:</b>
     * this is not the total number of records in those entities, but only the count
     * of those records from the primary data source.
     * 
     * @return The number of records from the primary data source in the entities
     *         described by the {@linkplain #getEntityCount() entityCount}.
     */
    public long getRecordCount() {
        return this.recordCount;
    }

    /**
     * Sets the number of records from the primary data source in the entities
     * described by the {@linkplain #getEntityCount() entityCount}. <b>NOTE:</b>
     * this is not the total number of records in those entities, but only the count
     * of those records from the primary data source.
     * 
     * @param recordCount The number of records from the primary data source in the
     *                    entities described by the {@linkplain #getEntityCount()
     *                    entityCount}.
     */
    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Gets the number of relationships of the respective relationship type between
     * entities having at least one record from the primary data source and entities
     * having at least one record from the "versus" data source.
     *
     * @return The number of relationships of the respective relationship type
     *         between entities having at least one record from the primary data
     *         source and entities having at least one record from the "versus" data
     *         source.
     */
    public long getRelationCount() {
        return this.relationCount;
    }

    /**
     * Sets the number of relationships of the respective relationship type between
     * entities having at least one record from the primary data source and entities
     * having at least one record from the "versus" data source.
     *
     * @param relationCount The number of relationships of the respective
     *                      relationship type between entities having at least one
     *                      record from the primary data source and entities having
     *                      at least one record from the "versus" data source.
     */
    public void setRelationCount(long relationCount) {
        this.relationCount = relationCount;
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "matchKey=[ " + this.getMatchKey() + " ], principle=[ " + this.getPrinciple() + " ], entityCount=[ "
                + this.getEntityCount() + " ], recordCount=[ " + this.getRecordCount() + " ], relationCount=[ "
                + this.getRelationCount() + " ]";
    }
}
