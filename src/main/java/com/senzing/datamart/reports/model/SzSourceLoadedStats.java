package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describes the count statistics for a specific data source.
 */
public class SzSourceLoadedStats implements Serializable {
    /**
     * The data source code
     */
    private String dataSource;

    /**
     * The number of records loaded for the data source.
     */
    private long recordCount = 0L;

    /**
     * The number of entities having at least one record from the associated data
     * source.
     */
    private long entityCount = 0L;

    /**
     * The number of records loaded for the data source that failed to match against
     * any other record in the repository.
     */
    private long unmatchedRecordCount = 0L;

    /**
     * Constructs with the specified data source code.
     * 
     * @param dataSourceCode The data source code identifying the data source to
     *                       which the statistics pertain.
     * 
     * @throws NullPointerException If the specified data source code is
     *                              <code>null</code>.
     */
    public SzSourceLoadedStats(String dataSourceCode) throws NullPointerException {
        Objects.requireNonNull(dataSourceCode, "Data source code cannot be null");
        this.dataSource = dataSourceCode;
        this.recordCount = 0L;
        this.entityCount = 0L;
        this.unmatchedRecordCount = 0L;
    }

    /**
     * Gets the data source code identifying the data source to which the statistics
     * are associated.
     *
     * @return The data source code identifying the data source to which the
     *         statistics are associated.
     */
    public String getDataSource() {
        return this.dataSource;
    }

    /**
     * Sets the data source code identifying the data source to which the statistics
     * are associated.
     *
     * @param dataSourceCode The non-null data source code identifying the data
     *                       source to which the statistics are associated.
     * 
     * @throws NullPointerException If the specified data source code is
     *                              <code>null</code>.
     */
    public void setDataSource(String dataSourceCode) {
        this.dataSource = dataSourceCode;
    }

    /**
     * Gets the total number of records that have been loaded for the associated
     * data source.
     *
     * @return The total number of records that have been loaded for the associated
     *         data source.
     */
    public long getRecordCount() {
        return this.recordCount;
    }

    /**
     * Sets the the total number of records that have been loaded for the associated
     * data source.
     *
     * @param recordCount The total number of records that have been loaded for the
     *                    associated data source.
     */
    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Gets the total number of entities that have at least one record from the
     * associated data source.
     *
     * @return The total number of entities that have at least one record from the
     *         associated data source.
     */
    public long getEntityCount() {
        return this.entityCount;
    }

    /**
     * Sets total number of entities that have at least one record from the
     * associated data source.
     *
     * @param entityCount The total number of entities that have been loaded for the
     *                    associated data source.
     */
    public void setEntityCount(long entityCount) {
        this.entityCount = entityCount;
    }

    /**
     * Gets the total number of entities that have at least one record from the
     * associated data source. This value doubles as the number of entities having a
     * record from the associated data source where that record is single
     * <b>only</b> record in the entity.
     *
     * @return The total number of entities that have at least one record from the
     *         associated data source.
     */
    public long getUnmatchedRecordCount() {
        return this.unmatchedRecordCount;
    }

    /**
     * Sets total number of records that have been loaded for the associated data
     * source that did <b>not</b> match against any other record. This value doubles
     * as the number of entities having a record from the associated data source
     * where that record is single <b>only</b> record in the entity.
     *
     * @param recordCount The total number of records that have been loaded for the
     *                    associated data source.
     */
    public void setUnmatchedRecordCount(long recordCount) {
        this.unmatchedRecordCount = recordCount;
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
                + " ], unmatchedRecordCount=[ " 
                + this.getUnmatchedRecordCount() + " ]";
    }

}
