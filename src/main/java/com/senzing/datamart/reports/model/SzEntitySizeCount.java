package com.senzing.datamart.reports.model;

import java.io.Serializable;

/**
 * Describes a the number of entities having a specific entity size given by the
 * {@linkplain #getEntitySize() record count}.
 */
public class SzEntitySizeCount implements Serializable {
    /**
     * The entity size described by the number of records in the entities.
     */
    private int entitySize = 0;

    /**
     * The number of entities having an entity size equal to the associated record
     * count.
     */
    private long entityCount = 0L;

    /**
     * Default constructor
     */
    public SzEntitySizeCount() {
        this.entitySize = 0;
        this.entityCount = 0L;
    }

    /**
     * Gets the number of records indicating the size of the entities for which the
     * entity count is provided.
     *
     * @return The number of records indicating the size of the entities for which
     *         the entity count is provided.
     */
    public int getEntitySize() {
        return this.entitySize;
    }

    /**
     * Sets the number of records indicating the size of the entities for which the
     * entity count is provided.
     *
     * @param recordCount The number of records indicating the size of the entities
     *                    for which the entity count is provided.
     */
    public void setEntitySize(int recordCount) {
        this.entitySize = recordCount;
    }

    /**
     * Gets number of entities in the entity repository having the associated
     * {@linkplain #getEntitySize() entity size}.
     *
     * @return The number of entities in the entity repository having the associated
     *         {@linkplain #getEntitySize() entity size}.
     */
    public long getEntityCount() {
        return this.entityCount;
    }

    /**
     * Sets number of entities in the entity repository having the associated
     * {@linkplain #getEntitySize() entity size}.
     *
     * @param entityCount The number of entities in the entity repository having the
     *                    associated {@linkplain #getEntitySize() entity size}.
     */
    public void setEntityCount(long entityCount) {
        this.entityCount = entityCount;
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "entitySize=[ " + this.getEntitySize() 
               + " ], entityCount=[ " + this.getEntityCount() +  " ]";
    }
}
