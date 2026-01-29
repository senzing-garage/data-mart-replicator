package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Describes the source summary and all cross-summaries with that data source.
 */
public class SzSourceSummary implements Serializable {
    /**
     * The data source to which the summary statistics apply.
     */
    private String dataSource;

    /**
     * The number of entities having at least one record from the data source.
     */
    private long entityCount = 0L;

    /**
     * The number of records loaded from the data source.
     */
    private long recordCount = 0L;

    /**
     * The number of records from this data source that did not match a record from
     * the same data source. This represents the number of records from the data
     * source that are effectively unique (not duplicated).
     */
    private long unmatchedRecordCount = 0L;

    /**
     * The {@link SortedMap} of {@link String} "versus" data source code keys to
     * {@link SzCrossSourceSummary} values describing the cross summary statistics
     * for that data source.
     */
    private SortedMap<String, SzCrossSourceSummary> crossSummaries = null;

    /**
     * Default constructor.
     */
    public SzSourceSummary() {
        this(null);
    }

    /**
     * Constructs with the specified data source code.
     * 
     * @param dataSourceCode The data source code to associated with the source
     *                       summary.
     * 
     */
    public SzSourceSummary(String dataSourceCode) {
        this.dataSource = dataSourceCode;
        this.recordCount = 0L;
        this.entityCount = 0L;
        this.unmatchedRecordCount = 0L;
        this.crossSummaries = new TreeMap<>();
    }

    /**
     * Gets the data source to which the summary statistics apply.
     *
     * @return The data source to which the summary statistics apply.
     */
    public String getDataSource() {
        return this.dataSource;
    }

    /**
     * Sets the data source to which the summary statistics apply.
     *
     * @param dataSource The data source to which the summary statistics apply.
     * 
     * @throws NullPointerException If the specified data source code is
     *                              <code>null</code>.
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Gets number of entities having at least one record from the data source.
     *
     * @return The number of entities having at least one record from the data
     *         source.
     */
    public long getEntityCount() {
        return this.entityCount;
    }

    /**
     * Sets number of entities having at least one record from the data source.
     *
     * @param entityCount The number of entities having at least one record from the
     *                    data source.
     */
    public void setEntityCount(long entityCount) {
        this.entityCount = entityCount;
    }

    /**
     * Gets the number of records loaded from the data source.
     *
     * @return The number of records loaded from the data source.
     */
    public long getRecordCount() {
        return this.recordCount;
    }

    /**
     * Sets number of records loaded from the data source.
     *
     * @param recordCount The number of records loaded from the data source.
     */
    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Gets number of records from this data source that did not match a record from
     * the same data source. This represents the number of records from the data
     * source that are effectively unique (not duplicated).
     *
     * @return The number of records from this data source that did not match a
     *         record from the same data source.
     */
    public long getUnmatchedRecordCount() {
        return this.unmatchedRecordCount;
    }

    /**
     * Sets the the total number of records that have been loaded to the entity
     * repository that did <b>not</b> match against any other record. This is also
     * the total number of entities that only have a single record.
     *
     * @param recordCount The total number of records that have been loaded to the
     *                    entity repository.
     */
    public void setUnmatchedRecordCount(long recordCount) {
        this.unmatchedRecordCount = recordCount;
    }

    /**
     * Gets the {@link List} of {@link SzCrossSourceSummary} instances describing
     * the summary statistics between the associated data source versus every data
     * source (including itself).
     * 
     * @return The {@link List} of {@link SzCrossSourceSummary} instances describing
     *         the summary statistics between the associated data source versus
     *         every data source (including itself).
     */
    public List<SzCrossSourceSummary> getCrossSourceSummaries() {
        return new ArrayList<>(this.crossSummaries.values());
    }

    /**
     * Sets the {@link List} of {@link SzCrossSourceSummary} instances describing
     * the summary statistics between the associated data source versus every data
     * source (including itself).
     * 
     * @param crossSummaries The {@link List} of {@link SzCrossSourceSummary}
     *                       instances describing the summary statistics between the
     *                       associated data source versus every data source.
     */
    public void setCrossSourceSummaries(Collection<SzCrossSourceSummary> crossSummaries) {
        this.crossSummaries.clear();
        if (crossSummaries != null) {
            crossSummaries.forEach(summary -> {
                if (summary != null) {
                    this.crossSummaries.put(summary.getVersusDataSource(), summary);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzCrossSourceSummary} describing the summary
     * statistics between the associated data source versus another specific data
     * source (which may be the same).
     * 
     * @param crossSummary The {@link SzCrossSourceSummary} describing the summary
     *                     statistics between the associated data source versus
     *                     another specific data source.
     */
    public void addCrossSourceSummary(SzCrossSourceSummary crossSummary) {
        if (crossSummary == null) {
            return;
        }
        this.crossSummaries.put(crossSummary.getVersusDataSource(), crossSummary);
    }

    /**
     * Removes the {@link SzCrossSourceSummary} associated with the specified versus
     * data source if any exists for that data source.
     * 
     * @param versusDataSourceCode The "versus" data sources code for which to
     *                             remove the cross source summary.
     */
    public void removeCrossSourceSummary(String versusDataSourceCode) {
        this.crossSummaries.remove(versusDataSourceCode);
    }

    /**
     * Removes all cross source summary statistics for all the data sources.
     */
    public void removeAllCrossSourceSummaries() {
        this.crossSummaries.clear();
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "dataSource=[ " + this.getDataSource()
            + " ], recordCount=[ " + this.getRecordCount()
            + " ], entityCount=[ " + this.getEntityCount()
            + " ], unmatchedRecordCount=[ " + this.getUnmatchedRecordCount()
            + " ], crossSourceSummaries=[ " + this.getCrossSourceSummaries()
            + " ]";
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)}
     * implementation.
     *
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(dataSource, entityCount, recordCount,
                            unmatchedRecordCount, crossSummaries);
    }

    /**
     * Overridden to return <code>true</code> if and only if the specified parameter
     * is a non-null reference to an object of the same class with equivalent properties.
     *
     * @param obj The object to compare with.
     *
     * @return <code>true</code> if the specified parameter is an instance of the
     *         same class with equivalent properties, otherwise <code>false</code>.
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
        SzSourceSummary other = (SzSourceSummary) obj;
        return Objects.equals(this.dataSource, other.dataSource)
                && this.entityCount == other.entityCount
                && this.recordCount == other.recordCount
                && this.unmatchedRecordCount == other.unmatchedRecordCount
                && Objects.equals(this.crossSummaries, other.crossSummaries);
    }
}
