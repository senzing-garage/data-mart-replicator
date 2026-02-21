package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.Objects;

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
     * Default constructor.
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
     * 
     * @throws IllegalArgumentException If the specified parameter is not positive.
     */
    public void setEntitySize(int recordCount) {
        if (recordCount < 1) {
            throw new IllegalArgumentException(
                "The specified record count must be a positive number: "
                + recordCount);
        }
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
     * 
     * @throws IllegalArgumentException If the specified parameter is negative.
     */
    public void setEntityCount(long entityCount) {
        if (entityCount < 0) {
            throw new IllegalArgumentException(
                "The specified entity count cannot be negative: "
                + entityCount);
        }
        this.entityCount = entityCount;
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "{ entitySize=[ " + this.getEntitySize() 
               + " ], entityCount=[ " + this.getEntityCount() +  " ] }";
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)} 
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(entitySize, entityCount);
    }

    /**
     * Overridden to return <code>true</code> if and only if the specified parameter
     * is an instance of the same class with equivalent properties.
     * 
     * @param obj The object to compare with.
     * @return <code>true</code> if the specified parameter is an instance of the 
     *         same class with equivalent properties, otherwise <code>false</code>.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SzEntitySizeCount)) {
            return false;
        }
        SzEntitySizeCount other = (SzEntitySizeCount) obj;
        return entitySize == other.entitySize && entityCount == other.entityCount;
    }
}
