package com.senzing.datamart.reports.model;

import java.io.Serializable;

/**
 * Describes a the number of entities having a specific number of entity
 * relations given by the {@linkplain #getRelationsCount() relations count}.
 */
public class SzEntityRelationsCount implements Serializable {
    /**
     * The number of entity relations that the entities have.
     */
    private int relationsCount = 0;

    /**
     * The number of entities having the associated number of entity relations.
     */
    private long entityCount = 0L;

    /**
     * Default constructor
     */
    public SzEntityRelationsCount() {
        this.relationsCount = 0;
        this.entityCount = 0L;
    }

    /**
     * Gets the number of entity relations for which the entity count is provided.
     *
     * @return The number of entity relations for which the entity count is
     *         provided.
     */
    public int getRelationsCount() {
        return this.relationsCount;
    }

    /**
     * Sets the number of entity relations for which the entity count is provided.
     *
     * @param relationsCount The number of entity relations for which the entity
     *                       count is provided.
     */
    public void setRelationsCount(int relationsCount) {
        this.relationsCount = relationsCount;
    }

    /**
     * Gets number of entities in the entity repository having the associated
     * {@linkplain #getRelationsCount() number of entity relations}.
     *
     * @return The number of entities in the entity repository having the associated
     *         {@linkplain #getRelationsCount() number of entity relations}.
     */
    public long getEntityCount() {
        return this.entityCount;
    }

    /**
     * Sets number of entities in the entity repository having the associated
     * {@linkplain #getRelationsCount() number of entity relations}.
     *
     * @param entityCount The number of entities in the entity repository having the
     *                    associated {@linkplain #getRelationsCount() number of
     *                    entity relations}.
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
        return "relationsCount=[ " + this.getRelationsCount() 
                + " ], entityCount=[ " + this.getEntityCount() + " ]";
    }
}
