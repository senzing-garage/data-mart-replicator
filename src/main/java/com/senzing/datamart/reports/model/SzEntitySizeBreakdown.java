package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Describes the number of entities in the entity repository at each 
 * count statistics for the repository.
 */
public class SzEntitySizeBreakdown implements Serializable {
    /**
     * The {@link Map} of {@link Integer} entity size keys to
     * {@link SzEntitySizeCount} values describing the number of entities in the
     * entity repository having that number of constituent records.
     */
    private Map<Integer, SzEntitySizeCount> entitySizeCounts = null;

    /**
     * Default constructor
     */
    public SzEntitySizeBreakdown() {
        this.entitySizeCounts = new LinkedHashMap<>();
    }

    /**
     * Gets the {@link List} of {@link SzEntitySizeCount} describing the number of
     * entities having each entity size indicated by the number of constituent
     * records for that entity. The returned value list should contain only one
     * element for each distinct entity size that exists in the repository. The
     * {@link List} is returned in descending order of entity size.
     * 
     * @return The {@link List} of {@link SzEntitySizeCount} describing the number
     *         of entities having each entity size indicated by the number of
     *         constituent records for that entity.
     */
    public List<SzEntitySizeCount> getEntitySizeCounts() {
        Collection<SzEntitySizeCount> sizeCounts = this.entitySizeCounts.values();
        List<SzEntitySizeCount> result = new ArrayList<>(sizeCounts);
        result.sort((c1, c2) -> {
            return c2.getEntitySize() - c1.getEntitySize();
        });
        return result;
    }

    /**
     * Sets the {@link List} of {@link SzEntitySizeCount} describing the number of
     * entities having each entity size indicated by the number of constituent
     * records for that entity. This clears any existing entity size counts before
     * setting with those specified. The specified {@link List} should contain only
     * one element for each entity size, but if duplicates are encountered then
     * later values in the {@link List} take precedence, overwriting prior values
     * from the {@link List}. Specifying a <code>null</code> {@link List} is
     * equivalent to specifying an empty {@link List}.
     * 
     * @param sizeCountList The {@link List} of {@link SzEntitySizeCount} describing
     *                      the number of entities having each entity size indicated
     *                      by the number of constituent records for that entity.
     */
    public void setEntitySizeCounts(List<SzEntitySizeCount> sizeCountList) {
        this.entitySizeCounts.clear();
        if (sizeCountList != null) {
            sizeCountList.forEach(sizeCount -> {
                if (sizeCount != null && sizeCount.getEntityCount() > 0) {
                    this.entitySizeCounts.put(sizeCount.getEntitySize(), sizeCount);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzEntitySizeCount} describing the number of
     * entities in the entity repository having a specific entity size. If the
     * specified {@link SzEntitySizeCount} has the same entity size as an existing
     * {@link SzEntitySizeCount} instance then the specified value replaces the
     * existing one for that entity size.
     * 
     * @param sizeCount The {@link SzEntitySizeCount} describing the number of
     *                  entities in the entity repository having a specific entity
     *                  size.
     */
    public void addEntitySizeCount(SzEntitySizeCount sizeCount) {
        if (sizeCount == null)
            return;
        if (sizeCount.getEntityCount() > 0) {
            this.entitySizeCounts.put(sizeCount.getEntitySize(), sizeCount);
        } else {
            this.entitySizeCounts.remove(sizeCount.getEntitySize());
        }
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "entitySizeCounts=[ " + this.getEntitySizeCounts() + " ]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(entitySizeCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SzEntitySizeBreakdown)) {
            return false;
        }
        SzEntitySizeBreakdown other = (SzEntitySizeBreakdown) obj;
        return Objects.equals(entitySizeCounts, other.entitySizeCounts);
    }
}
