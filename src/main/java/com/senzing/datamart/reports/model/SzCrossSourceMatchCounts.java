package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Describes the cross-source match statistics between two data sources.
 */
public class SzCrossSourceMatchCounts implements Serializable {
    /**
     * The primary data source in the cross comparison.
     */
    private String dataSource = null;

    /**
     * The versus data source in the cross comparison.
     */
    private String versusDataSource = null;

    /**
     * The {@link SortedMap} of {@link SzCountsKey} keys to {@link SzMatchCounts}
     * values for each requested match-key/principle combination that describe the
     * entity and record counts for matches between records from the primary data
     * source to at least one record from the "versus" data source.
     */
    private SortedMap<SzCountsKey, SzMatchCounts> counts = null;

    /**
     * Default constructor.
     */
    public SzCrossSourceMatchCounts() {
        this.dataSource = null;
        this.versusDataSource = null;
        this.counts = new TreeMap<>();
    }

    /**
     * Constructs with the primary and "versus" data source codes.
     * 
     * @param dataSource   The data source code for the primary data source.
     * @param vsDataSource The data source code for the "versus" data source.
     */
    public SzCrossSourceMatchCounts(String dataSource, String vsDataSource) {
        this.dataSource = dataSource;
        this.versusDataSource = vsDataSource;
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
     * Gets the {@link List} of {@link SzMatchCounts} instances for each requested
     * match key and principle combination that describe the entity and record
     * counts for matches between records from the primary data source to at least
     * one record from the "versus" data source.
     *
     * @return The {@link List} of {@link SzMatchCounts} instances for each
     *         requested match key and principle combination that describe the
     *         entity and record counts for matches for this instance.
     */
    public List<SzMatchCounts> getCounts() {
        return new ArrayList<>(this.counts.values());
    }

    /**
     * Sets the {@link SzMatchCounts} instances for this instance using the
     * specified {@link Collection} of {@link SzMatchCounts}. Any current
     * {@link SzMatchCounts} are removed and replaced with the specified instances.
     * If any of the {@link SzMatchCounts} instances have the same
     * match-key/principle pairs then the last one wins out replacing any previously
     * added ones.
     * 
     * @param matchCounts The {@link Collection} of {@link SzMatchCounts} instances
     *                    to set.
     */
    public void setCounts(Collection<SzMatchCounts> matchCounts) {
        this.counts.clear();
        if (matchCounts != null) {
            matchCounts.forEach(counts -> {
                SzCountsKey key = new SzCountsKey(counts.getMatchKey(), counts.getPrinciple());
                this.counts.put(key, counts);
            });
        }
    }

    /**
     * Adds the specified {@link SzMatchCounts} instance to the list of
     * {@link SzMatchCounts} instances describing the match counts for this
     * instance, replacing any existing instance with the same match key and
     * principle combination.
     * 
     * @param matchCounts The {@link SzMatchCounts} instance to add.
     */
    public void addCounts(SzMatchCounts matchCounts) {
        if (matchCounts == null) {
            return;
        }
        SzCountsKey key = new SzCountsKey(matchCounts.getMatchKey(), matchCounts.getPrinciple());
        this.counts.put(key, matchCounts);
    }

    /**
     * Removes the {@link SzMatchCounts} describing the match statistics associated
     * with the optionally specified match key and principle.
     * 
     * @param matchKey  The match key for the {@link SzMatchCounts} to remove, or
     *                  <code>null</code> if removing the statistics associated with
     *                  any match key.
     * @param principle The principle for the {@link SzMatchCounts} to remove, or
     *                  <code>null</code> if removing the statistics associated with
     *                  any principle.
     */
    public void removeCounts(String matchKey, String principle) {
        this.counts.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzMatchCounts} describing all the match statistics
     * associated with every combination of match key and principle.
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
                + " ], counts=[ " + this.getCounts() + " ]";
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)} 
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(dataSource, versusDataSource, counts);
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
        if (!(obj instanceof SzCrossSourceMatchCounts)) {
            return false;
        }
        SzCrossSourceMatchCounts other = (SzCrossSourceMatchCounts) obj;
        return Objects.equals(dataSource, other.dataSource) && Objects.equals(versusDataSource, other.versusDataSource)
                && Objects.equals(counts, other.counts);
    }
}
