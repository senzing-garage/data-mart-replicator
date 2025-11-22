package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Describes the cross-source statistics between two data sources.
 */
public class SzCrossSourceSummary implements Serializable {
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
    private SortedMap<SzCountsKey, SzMatchCounts> matches = null;

    /**
     * The {@link SortedMap} of {@link SzCountsKey} keys to {@link SzRelationCounts}
     * values for each requested match-key/principle combination that describes the
     * entity, record and relationship counts for ambiguous-match relationships
     * between entities having at least one record from the primary data source and
     * entities having at least one record from the "versus" data source.
     */
    private SortedMap<SzCountsKey, SzRelationCounts> ambiguousMatches = null;

    /**
     * The {@link SortedMap} of {@link SzCountsKey} keys to {@link SzRelationCounts}
     * values for each requested match-key/principle combination that describes the
     * entity, record and relationship counts for possible-match relationships
     * between entities having at least one record from the primary data source and
     * entities having at least one record from the "versus" data source.
     */
    private SortedMap<SzCountsKey, SzRelationCounts> possibleMatches = null;

    /**
     * The {@link SortedMap} of {@link SzCountsKey} keys to {@link SzRelationCounts}
     * values for each requested match-key/principle combination that describes the
     * entity, record and relationship counts for possible-relation relationships
     * between entities having at least one record from the primary data source and
     * entities having at least one record from the "versus" data source.
     */
    private SortedMap<SzCountsKey, SzRelationCounts> possibleRelations = null;

    /**
     * The {@link SortedMap} of {@link SzCountsKey} keys to {@link SzRelationCounts}
     * values for each requested match-key/principle combination that describes the
     * entity, record and relationship counts for disclosed-relation relationships
     * between entities having at least one record from the primary data source and
     * entities having at least one record from the "versus" data source.
     */
    private SortedMap<SzCountsKey, SzRelationCounts> disclosedRelations = null;

    /**
     * Default constructor.
     */
    public SzCrossSourceSummary() {
        this.dataSource = null;
        this.versusDataSource = null;
        this.matches = new TreeMap<>();
        this.ambiguousMatches = new TreeMap<>();
        this.possibleMatches = new TreeMap<>();
        this.possibleRelations = new TreeMap<>();
        this.disclosedRelations = new TreeMap<>();
    }

    /**
     * Constructs with the primary and "versus" data source codes.
     * 
     * @param dataSource   The data source code for the primary data source.
     * @param vsDataSource The data source code for the "versus" data source.
     */
    public SzCrossSourceSummary(String dataSource, String vsDataSource) {
        this.dataSource = dataSource;
        this.versusDataSource = vsDataSource;
        this.matches = new TreeMap<>();
        this.ambiguousMatches = new TreeMap<>();
        this.possibleMatches = new TreeMap<>();
        this.possibleRelations = new TreeMap<>();
        this.disclosedRelations = new TreeMap<>();
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
    public List<SzMatchCounts> getMatches() {
        return new ArrayList<>(this.matches.values());
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
    public void setMatches(Collection<SzMatchCounts> matchCounts) {
        this.matches.clear();
        if (matchCounts != null) {
            matchCounts.forEach(counts -> {
                SzCountsKey key = new SzCountsKey(counts.getMatchKey(), counts.getPrinciple());
                this.matches.put(key, counts);
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
    public void addMatches(SzMatchCounts matchCounts) {
        if (matchCounts == null)
            return;
        SzCountsKey key = new SzCountsKey(matchCounts.getMatchKey(), matchCounts.getPrinciple());
        this.matches.put(key, matchCounts);
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
    public void removeMatches(String matchKey, String principle) {
        this.matches.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzMatchCounts} describing all the match statistics
     * associated with every combination of match key and principle.
     */
    public void removeAllMatches() {
        this.matches.clear();
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
    public List<SzRelationCounts> getAmbiguousMatches() {
        return new ArrayList<>(this.ambiguousMatches.values());
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
    public void setAmbiguousMatches(Collection<SzRelationCounts> relationCounts) {
        this.ambiguousMatches.clear();
        if (relationCounts != null) {
            relationCounts.forEach(counts -> {
                SzCountsKey key = new SzCountsKey(counts.getMatchKey(), counts.getPrinciple());
                this.ambiguousMatches.put(key, counts);
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
    public void addAmbiguousMatches(SzRelationCounts relationCounts) {
        if (relationCounts == null)
            return;
        SzCountsKey key = new SzCountsKey(relationCounts.getMatchKey(), relationCounts.getPrinciple());
        this.ambiguousMatches.put(key, relationCounts);
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
    public void removeAmbiguousMatches(String matchKey, String principle) {
        this.ambiguousMatches.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzRelationCounts} describing all the ambiguous match
     * statistics associated with every combination of match key and principle.
     */
    public void removeAllAmbiguousMatches() {
        this.ambiguousMatches.clear();
    }

    /**
     * Gets the {@link List} of {@link SzRelationCounts} instances for each
     * requested match key and principle combination that describe the entity,
     * record and relationship counts for possible-match relationships between
     * entities having at least one record from the primary data source and entities
     * having at least one record from the "versus" data source.
     *
     * @return The {@link List} of {@link SzRelationCounts} instances for each
     *         requested match key and principle combination describing the
     *         possible-match entity, record and relationship counts for this
     *         instance.
     */
    public List<SzRelationCounts> getPossibleMatches() {
        return new ArrayList<>(this.possibleMatches.values());
    }

    /**
     * Sets the {@link SzRelationCounts} instances describing the possible-match
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
    public void setPossibleMatches(Collection<SzRelationCounts> relationCounts) {
        this.possibleMatches.clear();
        if (relationCounts != null) {
            relationCounts.forEach(counts -> {
                SzCountsKey key = new SzCountsKey(counts.getMatchKey(), counts.getPrinciple());
                this.possibleMatches.put(key, counts);
            });
        }
    }

    /**
     * Adds the specified {@link SzRelationCounts} instance to the list of
     * {@link SzRelationCounts} instances describing the possible-match relationship
     * counts for this instance, replacing any existing instance with the same match
     * key and principle combination.
     * 
     * @param relationCounts The {@link SzRelationCounts} instance to add.
     */
    public void addPossibleMatches(SzRelationCounts relationCounts) {
        if (relationCounts == null)
            return;
        SzCountsKey key = new SzCountsKey(relationCounts.getMatchKey(), relationCounts.getPrinciple());
        this.possibleMatches.put(key, relationCounts);
    }

    /**
     * Removes the {@link SzRelationCounts} describing the possible match statistics
     * associated with the optionally specified match key and principle.
     * 
     * @param matchKey  The match key for the possible match
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any match key.
     * @param principle The principle for the possible match
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any principle.
     */
    public void removePossibleMatches(String matchKey, String principle) {
        this.possibleMatches.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzRelationCounts} describing all the possible match
     * statistics associated with every combination of match key and principle.
     */
    public void removeAllPossibleMatches() {
        this.possibleMatches.clear();
    }

    /**
     * Gets the {@link List} of {@link SzRelationCounts} instances for each
     * requested match key and principle combination that describe the entity,
     * record and relationship counts for possible-relation relationships between
     * entities having at least one record from the primary data source and entities
     * having at least one record from the "versus" data source.
     *
     * @return The {@link List} of {@link SzRelationCounts} instances for each
     *         requested match key and principle combination describing the
     *         possible-relation entity, record and relationship counts for this
     *         instance.
     */
    public List<SzRelationCounts> getPossibleRelations() {
        return new ArrayList<>(this.possibleRelations.values());
    }

    /**
     * Sets the {@link SzRelationCounts} instances describing the possible-relation
     * counts for one or more match-key/principle combination using the specified
     * {@link Collection} of {@link SzRelationCounts}. Any current
     * {@link SzRelationCounts} are removed and replaced with the specified
     * instances. If any of the {@link SzRelationCounts} instances have the same
     * match-key/principle pairs then the last one wins out replacing any previously
     * added ones.
     * 
     * @param relationCounts The {@link Collection} of {@link SzRelationCounts}
     *                       instances to set.
     */
    public void setPossibleRelations(Collection<SzRelationCounts> relationCounts) {
        this.possibleRelations.clear();
        if (relationCounts != null) {
            relationCounts.forEach(counts -> {
                SzCountsKey key = new SzCountsKey(counts.getMatchKey(), counts.getPrinciple());
                this.possibleRelations.put(key, counts);
            });
        }
    }

    /**
     * Adds the specified {@link SzRelationCounts} instance to the list of
     * {@link SzRelationCounts} instances describing the possible-relation
     * relationship counts for this instance, replacing any existing instance with
     * the same match key and principle combination.
     * 
     * @param relationCounts The {@link SzRelationCounts} instance to add.
     */
    public void addPossibleRelations(SzRelationCounts relationCounts) {
        if (relationCounts == null)
            return;
        SzCountsKey key = new SzCountsKey(relationCounts.getMatchKey(), relationCounts.getPrinciple());
        this.possibleRelations.put(key, relationCounts);
    }

    /**
     * Removes the {@link SzRelationCounts} describing the possible relation
     * statistics associated with the optionally specified match key and principle.
     * 
     * @param matchKey  The match key for the possible relations
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any match key.
     * @param principle The principle for the possible relations
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any principle.
     */
    public void removePossibleRelations(String matchKey, String principle) {
        this.possibleRelations.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzRelationCounts} describing all the possible relation
     * statistics associated with every combination of match key and principle.
     */
    public void removeAllPossibleRelations() {
        this.possibleRelations.clear();
    }

    /**
     * Gets the {@link List} of {@link SzRelationCounts} instances for each
     * requested match key and principle combination that describe the entity,
     * record and relationship counts for disclosed-relation relationships between
     * entities having at least one record from the primary data source and entities
     * having at least one record from the "versus" data source.
     *
     * @return The {@link List} of {@link SzRelationCounts} instances for each
     *         requested match key and principle combination describing the
     *         disclosed-relation entity, record and relationship counts for this
     *         instance.
     */
    public List<SzRelationCounts> getDisclosedRelations() {
        return new ArrayList<>(this.disclosedRelations.values());
    }

    /**
     * Sets the {@link SzRelationCounts} instances describing the disclosed-relation
     * counts for one or more match-key/principle combination using the specified
     * {@link Collection} of {@link SzRelationCounts}. Any current
     * {@link SzRelationCounts} are removed and replaced with the specified
     * instances. If any of the {@link SzRelationCounts} instances have the same
     * match-key/principle pairs then the last one wins out replacing any previously
     * added ones.
     * 
     * @param relationCounts The {@link Collection} of {@link SzRelationCounts}
     *                       instances to set.
     */
    public void setDisclosedRelations(Collection<SzRelationCounts> relationCounts) {
        this.disclosedRelations.clear();
        if (relationCounts != null) {
            relationCounts.forEach(counts -> {
                SzCountsKey key = new SzCountsKey(counts.getMatchKey(), counts.getPrinciple());
                this.disclosedRelations.put(key, counts);
            });
        }
    }

    /**
     * Adds the specified {@link SzRelationCounts} instance to the list of
     * {@link SzRelationCounts} instances describing the disclosed-relation
     * relationship counts for this instance, replacing any existing instance with
     * the same match key and principle combination.
     * 
     * @param relationCounts The {@link SzRelationCounts} instance to add.
     */
    public void addDisclosedRelations(SzRelationCounts relationCounts) {
        if (relationCounts == null)
            return;
        SzCountsKey key = new SzCountsKey(relationCounts.getMatchKey(), relationCounts.getPrinciple());
        this.disclosedRelations.put(key, relationCounts);
    }

    /**
     * Removes the {@link SzRelationCounts} describing the disclosed relation
     * statistics associated with the optionally specified match key and principle.
     * 
     * @param matchKey  The match key for the disclosed relations
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any match key.
     * @param principle The principle for the disclosed relations
     *                  {@link SzRelationCounts} to remove, or <code>null</code> if
     *                  removing the statistics associated with any principle.
     */
    public void removeDisclosedRelations(String matchKey, String principle) {
        this.disclosedRelations.remove(new SzCountsKey(matchKey, principle));
    }

    /**
     * Removes all the {@link SzRelationCounts} describing all the disclosed
     * relation statistics associated with every combination of match key and
     * principle.
     */
    public void removeAllDisclosedRelations() {
        this.disclosedRelations.clear();
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
                + " ], matches=[ " + this.getMatches()
                + " ], ambiguousMatches=[ " + this.getAmbiguousMatches()
                + " ], possibleMatches=[ " + this.getPossibleMatches()
                + " ], possibleRelations=[ " + this.getPossibleRelations()
                + " ], disclosedRelations=[ " + this.getDisclosedRelations() + " ]";
    }
}
