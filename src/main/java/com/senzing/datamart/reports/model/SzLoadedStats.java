package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Describes a count statistics for the repository.
 */
public class SzLoadedStats implements Serializable {
    /**
     * The total number of records loaded in the entity repository.
     */
    private long totalRecordCount = 0L;

    /**
     * The total number of entities that have been resolved in the entity
     * repository.
     */
    private long totalEntityCount = 0L;

    /**
     * The total number of records loaded in the entity repository that failed to
     * match against any other record. This is also the number of entities in the
     * repository that only have a single record.
     */
    private long totalUnmatchedRecordCount = 0L;

    /**
     * The {@link Map} of {@link String} data source code keys to
     * {@link SzSourceLoadedStats} values describing the count statistics for that
     * data source.
     */
    private SortedMap<String, SzSourceLoadedStats> dataSourceCounts = null;

    /**
     * Default constructor
     */
    public SzLoadedStats() {
        this.totalRecordCount = 0L;
        this.totalEntityCount = 0L;
        this.totalUnmatchedRecordCount = 0L;
        this.dataSourceCounts = new TreeMap<>();
    }

    /**
     * Gets the total number of records that have been loaded to the entity
     * repository.
     *
     * @return The total number of records that have been loaded to the entity
     *         repository.
     */
    public long getTotalRecordCount() {
        return this.totalRecordCount;
    }

    /**
     * Sets the the total number of records that have been loaded to the entity
     * repository.
     *
     * @param recordCount The total number of records that have been loaded to the
     *                    entity repository.
     */
    public void setTotalRecordCount(long recordCount) {
        this.totalRecordCount = recordCount;
    }

    /**
     * Gets the total number of entities that have been resolved in the entity
     * repository.
     *
     * @return The total number of entities that have been resolved in the entity
     *         repository.
     */
    public long getTotalEntityCount() {
        return this.totalEntityCount;
    }

    /**
     * Sets total number of entities that have at least one record from the
     * associated data source.
     *
     * @param entityCount The total number of entities that have been loaded for the
     *                    associated data source.
     */
    public void setTotalEntityCount(long entityCount) {
        this.totalEntityCount = entityCount;
    }

    /**
     * Gets the total number of records that have been loaded to the entity
     * repository that did <b>not</b> match against any other records. This is also
     * the total number of entities that only have a single record.
     *
     * @return The total number of records that have been loaded to the entity
     *         repository.
     */
    public long getTotalUnmatchedRecordCount() {
        return this.totalUnmatchedRecordCount;
    }

    /**
     * Sets the the total number of records that have been loaded to the entity
     * repository that did <b>not</b> match against any other record. This is also
     * the total number of entities that only have a single record.
     *
     * @param recordCount The total number of records that have been loaded to the
     *                    entity repository.
     */
    public void setTotalUnmatchedRecordCount(long recordCount) {
        this.totalUnmatchedRecordCount = recordCount;
    }

    /**
     * Gets the {@link List} of {@link SzSourceLoadedStats} describing the count
     * statistics for each data source. The returned value list should contain only
     * one element for each data source.
     * 
     * @return The {@link List} of {@link SzSourceLoadedStats} describing the count
     *         statistics for each data source. The returned {@link List} should
     *         contain only one element for each data source.
     */
    public List<SzSourceLoadedStats> getDataSourceCounts() {
        Collection<SzSourceLoadedStats> stats = this.dataSourceCounts.values();
        return new ArrayList<>(stats);
    }

    /**
     * Sets the {@link List} of {@link SzSourceLoadedStats} describing the count
     * statistics for each data source. This clears any existing data source counts
     * before setting with those specified. The specified {@link List} should
     * contain only one element for each data source, but if duplicates are
     * encountered then later values in the {@link List} take precedence,
     * overwriting prior values from the {@link List}. Specifying a
     * <code>null</code> {@link List} is equivalent to specifying an empty
     * {@link List}.
     * 
     * @param statsList The {@link List} of {@link SzSourceLoadedStats} describing
     *                  the count statistics for each data source.
     */
    public void setDataSourceCounts(List<SzSourceLoadedStats> statsList) {
        this.dataSourceCounts.clear();
        if (statsList != null) {
            statsList.forEach(stats -> {
                if (stats != null) {
                    this.dataSourceCounts.put(stats.getDataSource(), stats);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzSourceLoadedStats} describing count statistics
     * for a data source to the existing {@link SzSourceLoadedStats} for this
     * instance. If the specified {@link SzSourceLoadedStats} has the same data
     * source code as an existing {@link SzSourceLoadedStats} instance then the
     * specified value replaces the existing one for that data source code.
     * 
     * @param stats The {@link SzSourceLoadedStats} describing count statistics for
     *              a specific data source.
     */
    public void addDataSourceCount(SzSourceLoadedStats stats) {
        if (stats == null)
            return;
        this.dataSourceCounts.put(stats.getDataSource(), stats);
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "totalRecordCount=[ " + this.getTotalRecordCount() 
                + " ], totalEntityCount=[ " + this.getTotalEntityCount()
                + " ], totalUnmatchedRecordCount=[ " 
                + this.getTotalUnmatchedRecordCount()
                + " ], dataSourceCounts=[ " + this.getDataSourceCounts() + " ]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRecordCount, totalEntityCount, totalUnmatchedRecordCount, dataSourceCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SzLoadedStats)) {
            return false;
        }
        SzLoadedStats other = (SzLoadedStats) obj;
        return totalRecordCount == other.totalRecordCount && totalEntityCount == other.totalEntityCount
                && totalUnmatchedRecordCount == other.totalUnmatchedRecordCount
                && Objects.equals(dataSourceCounts, other.dataSourceCounts);
    }

}
