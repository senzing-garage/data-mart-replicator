package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Provides a key for uniquely identifying relationships and preventing
 * duplication.
 */
class SzRelationKey implements Comparable<SzRelationKey>, Serializable {
    /**
     * The first entity ID.
     */
    private long entityId;

    /**
     * The related entity ID.
     */
    private long relatedId;

    /**
     * Constructs with the two entity ID's.
     * 
     * @param entityId  The first entity ID.
     * @param relatedId The related entity ID.
     */
    SzRelationKey(long entityId, long relatedId) {
        this.entityId = entityId;
        this.relatedId = relatedId;
    }

    /**
     * Gets the first entity ID of the relationship.
     * 
     * @return The first entity ID of the relationship.
     */
    public long getEntityId() {
        return this.entityId;
    }

    /**
     * Gets the related entity ID of the relationship.
     * 
     * @return The related entity ID of the relationship.
     */
    public long getRelatedId() {
        return this.relatedId;
    }

    /**
     * Implemented to return <code>true</code> if and only if the specified
     * parameter is a non-null reference to an object of the same class with
     * equivalent entity ID and related ID.
     * 
     * @param obj The object to compare with.
     * @return <code>true</code> if the objects are equal, otherwise
     *         <code>false</code>.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SzRelationKey key = (SzRelationKey) obj;
        return this.getEntityId() == key.getEntityId() && this.getRelatedId() == key.getRelatedId();
    }

    /**
     * Implemented to return a hash code based on the entity ID and the related
     * entity ID.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getEntityId(), this.getRelatedId());
    }

    /**
     * Implemented to compare by sorting first on entity ID and then on related
     * entity ID (both in ascending order) with <code>null</code> values sorted
     * before non-null values.
     * 
     * @return A negative number, zero (0) or a positive number depending on whether
     *         this instance compares less-than, equal-to or greater-than the
     *         specified instance, respectively.
     */
    @Override
    public int compareTo(SzRelationKey key) {
        if (key == null) {
            return 1;
        }
        long diff = this.getEntityId() - key.getEntityId();
        if (diff < 0L) {
            return -1;
        }
        if (diff > 0L) {
            return 1;
        }

        diff = this.getRelatedId() - key.getRelatedId();
        if (diff < 0L) {
            return -1;
        }
        if (diff > 0L) {
            return 1;
        }
        return 0;
    }

    /**
     * Implemented to format this instance as a relation bound value with the entity
     * ID followed by the related ID, separated by a colon.
     * 
     * @return A relation bound value with the entity ID followed by the related ID,
     *         separated by a colon.
     */
    public String toString() {
        return this.getEntityId() + ":" + this.getRelatedId();
    }
}
