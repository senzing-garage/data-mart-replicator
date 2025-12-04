package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Describes the number of entities in the entity repository at each count
 * statistics for the repository.
 */
public class SzEntityRelationsBreakdown implements Serializable {
    /**
     * The {@link Map} of {@link Integer} keys indicating the number of entity
     * relations to {@link SzEntityRelationsCount} values describing the number of
     * entities in the entity repository having that number entity relations.
     */
    private Map<Integer, SzEntityRelationsCount> relationsCounts = null;

    /**
     * Default constructor.
     */
    public SzEntityRelationsBreakdown() {
        this.relationsCounts = new LinkedHashMap<>();
    }

    /**
     * Gets the {@link List} of {@link SzEntityRelationsCount} describing the number
     * of entities having each distinct number of entity relations. The returned
     * value list should contain only one element for each distinct number of entity
     * relations that exists in the repository. The {@link List} is returned in
     * descending order of the number of entity relations.
     * 
     * @return The {@link List} of {@link SzEntityRelationsCount} describing the
     *         number of entities having each distinct number of entity relations.
     */
    public List<SzEntityRelationsCount> getEntityRelationsCounts() {
        Collection<SzEntityRelationsCount> relationsCounts = this.relationsCounts.values();
        List<SzEntityRelationsCount> result = new ArrayList<>(relationsCounts);
        result.sort((c1, c2) -> {
            return c2.getRelationsCount() - c1.getRelationsCount();
        });
        return result;
    }

    /**
     * Sets the {@link List} of {@link SzEntityRelationsCount} describing the number
     * of entities having each distinct number of entity relations. This clears any
     * existing entity relations counts before setting with those specified. The
     * specified {@link List} should contain only one element for each distinct
     * number of entity relations, but if duplicates are encountered then later
     * values in the {@link List} take precedence, overwriting prior values from the
     * {@link List}. Specifying a <code>null</code> {@link List} is equivalent to
     * specifying an empty {@link List}.
     * 
     * @param relationsCountList The {@link List} of {@link SzEntityRelationsCount}
     *                           describing the number of entities having each
     *                           distinct number of entity relations.
     */
    public void setEntityRelationsCounts(List<SzEntityRelationsCount> relationsCountList) {
        this.relationsCounts.clear();
        if (relationsCountList != null) {
            relationsCountList.forEach(relationsCount -> {
                if (relationsCount != null && relationsCount.getEntityCount() > 0) {
                    this.relationsCounts.put(relationsCount.getRelationsCount(), relationsCount);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzEntityRelationsCount} describing the number of
     * entities in the entity repository having a specific number of entity
     * relations. If the specified {@link SzEntityRelationsCount} has the same
     * entity relations count as an existing {@link SzEntityRelationsCount} instance
     * then the specified value replaces the existing one for that number of entity
     * relations.
     * 
     * @param relationsCount The {@link SzEntityRelationsCount} describing the
     *                       number of entities in the entity repository having a
     *                       specific number of entity relations.
     */
    public void addEntityRelationsCount(SzEntityRelationsCount relationsCount) {
        if (relationsCount == null) {
            return;
        }
        if (relationsCount.getEntityCount() > 0) {
            this.relationsCounts.put(relationsCount.getRelationsCount(), relationsCount);
        } else {
            this.relationsCounts.remove(relationsCount.getRelationsCount());
        }
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "relationCounts=[ " + this.getEntityRelationsCounts() + " ]";
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)} 
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(relationsCounts);
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
        if (!(obj instanceof SzEntityRelationsBreakdown)) {
            return false;
        }
        SzEntityRelationsBreakdown other = (SzEntityRelationsBreakdown) obj;
        return Objects.equals(relationsCounts, other.relationsCounts);
    }
}
