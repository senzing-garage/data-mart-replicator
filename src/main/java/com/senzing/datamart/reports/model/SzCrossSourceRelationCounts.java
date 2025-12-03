package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Describes the cross-source relation statistics between two data sources.
 */
public class SzCrossSourceRelationCounts implements Serializable {
    /**
     * The primary data source in the cross comparison.
     */
    private String dataSource = null;

    /**
     * The versus data source in the cross comparison.
     */
    private String versusDataSource = null;

    /**
     * The {@link SzRelationType} for the {@link SzRelationCounts} instances.
     */
    private SzRelationType relationType = null;

    /**
     * The {@link SortedMap} of {@link SzCountsKey} keys to {@link SzRelationCounts}
     * values for each requested match-key/principle combination that describes the
     * entity, record and relationship counts for ambiguous-match relationships
     * between entities having at least one record from the primary data source and
     * entities having at least one record from the "versus" data source.
     */
    private SortedMap<SzCountsKey, SzRelationCounts> counts = null;

    /**
     * Default constructor.
     */
    public SzCrossSourceRelationCounts() {
        this(null, null, null);
    }

    /**
     * Constructs with the primary and "versus" data source codes.
     * 
     * @param dataSource   The data source code for the primary data source.
     * @param vsDataSource The data source code for the "versus" data source.
     * @param relationType The {@link SzRelationType} of the relations being
     *                     counted.
     */
    public SzCrossSourceRelationCounts(String dataSource, String vsDataSource, SzRelationType relationType) {
        this.dataSource = dataSource;
        this.versusDataSource = vsDataSource;
        this.relationType = relationType;
        this.counts = new TreeMap<>();
    }

    /**
     * Gets the primary data source in the cross comparison.
     *
     * @return The primary data source in the cross comparison.
     */
    public String getDataSource() {
        return this.dataSource;
    }

    /**
     * Sets the primary data source in the cross comparison.
     *
     * @param dataSource The non-null primary data source in the cross comparison.
     * 
     * @throws NullPointerException If the specified data source code is
     *                              <code>null</code>.
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Gets the versus data source in the cross comparison.
     *
     * @return The versus data source in the cross comparison.
     */
    public String getVersusDataSource() {
        return this.versusDataSource;
    }

    /**
     * Sets the versus data source in the cross comparison.
     *
     * @param dataSource The non-null versus data source in the cross comparison.
     * 
     * @throws NullPointerException If the specified data source code is
     *                              <code>null</code>.
     */
    public void setVersusDataSource(String dataSource) {
        this.versusDataSource = dataSource;
    }

    /**
     * Gets the {@link SzRelationType} describing the type of relationship match for
     * the associated statistics.
     * 
     * @return The {@link SzRelationType} describing the type of relationship match
     *         for the associated statistics.
     */
    public SzRelationType getRelationType() {
        return this.relationType;
    }

    /**
     * Sets the {@link SzRelationType} describing the type of relationship match for
     * the associated statistics.
     * 
     * @param relationType The {@link SzRelationType} describing the type of
     *                     relationship match for the associated statistics.
     */
    public void setRelationType(SzRelationType relationType) {
        this.relationType = relationType;
    }

    /**
     * Gets the {@link List} of {@link SzRelationCounts} instances for each
     * requested match key and principle combination that describe the entity,
     * record and relationship counts for ambiguous-match relationships between
     * entities having at least one record from the primary data source and entities
     * having at least one record from the "versus" data source.
     *
     * @return The {@link List} of {@link SzRelationCounts} instances for each
     *         requested match key and principle combination describing the
     *         ambiguous-match entity, record and relationship counts for this
     *         instance.
     */
    public List<SzRelationCounts> getCounts() {
        return new ArrayList<>(this.counts.values());
    }

    /**
     * Sets the {@link SzRelationCounts} instances describing the ambiguous match
     * relation counts for one or more match-key/principle combination using the
     * specified {@link Collection} of {@link SzRelationCounts}. Any current
     * {@link SzRelationCounts} are removed and replaced with the specified
     * instances. If any of the {@link SzRelationCounts} instances have the same
     * match-key/principle pairs then the last one wins out replacing any previously
     * added ones.
     * 
     * @param relationCounts The {@link Collection} of {@link SzRelationCounts}
     *                       instances to set.
     */
    public void setCounts(Collection<SzRelationCounts> relationCounts) {
        this.counts.clear();
        if (relationCounts != null) {
            relationCounts.forEach(relCounts -> {
                SzCountsKey key = new SzCountsKey(relCounts.getMatchKey(), relCounts.getPrinciple());
                this.counts.put(key, relCounts);
            });
        }
    }

    /**
     * Adds the specified {@link SzRelationCounts} instance to the list of
     * {@link SzRelationCounts} instances describing the ambiguous-match
     * relationship counts for this instance, replacing any existing instance with
     * the same match key and principle combination.
     * 
     * @param relationCounts The {@link SzRelationCounts} instance to add.
     */
    public void addCounts(SzRelationCounts relationCounts) {
        if (relationCounts == null)
            return;
        SzCountsKey key = new SzCountsKey(relationCounts.getMatchKey(), relationCounts.getPrinciple());
        this.counts.put(key, relationCounts);
    }

    /**
     * Removes the {@link SzRelationCounts} describing the ambiguous match
     * statistics associated with the optionally specified match key and principle.
     * 
     * @param matchKey  The match key for the ambiguous match
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any match key.
     * @param principle The principle for the ambiguous match
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any principle.
     */
    public void removeCounts(String matchKey, String principle) {
        this.counts.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzRelationCounts} describing all the ambiguous match
     * statistics associated with every combination of match key and principle.
     */
    public void removeAllCounts() {
        this.counts.clear();
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "dataSource=[ " + this.getDataSource() 
                + " ], versusDataSource=[ " + this.getVersusDataSource()
                + " ], relationType=[ " + this.getRelationType()
                + " ], counts=[ " + this.getCounts() + " ]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSource, versusDataSource, relationType, counts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SzCrossSourceRelationCounts)) {
            return false;
        }
        SzCrossSourceRelationCounts other = (SzCrossSourceRelationCounts) obj;
        return Objects.equals(dataSource, other.dataSource) && Objects.equals(versusDataSource, other.versusDataSource)
                && relationType == other.relationType && Objects.equals(counts, other.counts);
    }
}
