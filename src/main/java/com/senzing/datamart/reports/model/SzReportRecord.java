package com.senzing.datamart.reports.model;

import java.io.Serializable;

/**
 * Describes a relationship between two entities.
 */
public class SzReportRecord implements Serializable {
    /**
     * The data source code identifying the data source from which the record was
     * loaded.
     */
    private String dataSource = null;

    /**
     * The record ID that uniquely identifies the record within the data source from
     * which it was loaded.
     */
    private String recordId = null;

    /**
     * The optional match key describing why the record merged into the entity to
     * which it belongs. This is <code>null</code> if this record belongs to a
     * single-record entity or if it was the initial record of the first
     * multi-record entity to which it belonged (even if it later re-resolved into a
     * larger entity).
     */
    private String matchKey = null;

    /**
     * The optional principle describing why the record merged into the entity to
     * which it belongs. This is <code>null</code> if this record belongs to a
     * single-record entity or if it was the initial record of the first
     * multi-record entity to which it belonged (even if it later re-resolved into a
     * larger entity).
     */
    private String principle = null;

    /**
     * Default constructor.
     */
    public SzReportRecord() {
        this(null, null);
    }

    /**
     * Constructs with the specified parameters.
     * 
     * @param dataSourceCode The data source code identifying the data source from
     *                       which the record was loaded.
     * @param recordId       The record ID that uniquely identifies the record
     *                       within the data source from which it was loaded.
     */
    public SzReportRecord(String dataSourceCode, String recordId) {
        this.dataSource = dataSourceCode;
        this.recordId = recordId;
    }

    /**
     * Gets the data source code identifying the data source from which the record
     * was loaded.
     * 
     * @return The data source code identifying the data source from which the
     *         record was loaded.
     */
    public String getDataSource() {
        return this.dataSource;
    }

    /**
     * Gets the data source code identifying the data source from which the record
     * was loaded.
     * 
     * @param dataSourceCode The data source code identifying the data source from
     *                       which the record was loaded.
     */
    public void setDataSource(String dataSourceCode) {
        this.dataSource = dataSourceCode;
    }

    /**
     * Gets the record ID that uniquely identifies the record within the data source
     * from which it was loaded.
     * 
     * @return The record ID that uniquely identifies the record within the data
     *         source from which it was loaded.
     */
    public String getRecordId() {
        return this.recordId;
    }

    /**
     * Sets the record ID that uniquely identifies the record within the data source
     * from which it was loaded.
     * 
     * @param recordId The record ID that uniquely identifies the record within the
     *                 data source from which it was loaded.
     */
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    /**
     * Gets the optional match key describing why the record merged into the entity
     * to which it belongs. This returns <code>null</code> if this record belongs to
     * a single-record entity or if it was the initial record of the first
     * multi-record entity to which it belonged (even if it later re-resolved into a
     * larger entity).
     * 
     * @return The optional match key describing why the record merged into the
     *         entity to which it belongs, or <code>null</code> if this record
     *         belongs to a single-record entity or if it was the initial record of
     *         the first multi-record entity to which it belonged.
     */
    public String getMatchKey() {
        return this.matchKey;
    }

    /**
     * Sets the optional match key describing why the record merged into the entity
     * to which it belongs. Set this to <code>null</code> if this record belongs to
     * a single-record entity or if it was the initial record of the first
     * multi-record entity to which it belonged (even if it later re-resolved into a
     * larger entity).
     * 
     * @param matchKey The optional match key describing why the record merged into
     *                 the entity to which it belongs, or <code>null</code> if this
     *                 record belongs to a single-record entity or if it was the
     *                 initial record of the first multi-record entity to which it
     *                 belonged.
     */
    public void setMatchKey(String matchKey) {
        this.matchKey = matchKey;
    }

    /**
     * Gets the optional principle describing why the record merged into the entity
     * to which it belongs. This returns <code>null</code> if this record belongs to
     * a single-record entity or if it was the initial record of the first
     * multi-record entity to which it belonged (even if it later re-resolved into a
     * larger entity).
     * 
     * @return The optional principle describing why the record merged into the
     *         entity to which it belongs, or <code>null</code> if this record
     *         belongs to a single-record entity or if it was the initial record of
     *         the first multi-record entity to which it belonged.
     */
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * Sets the optional principle describing why the record merged into the entity
     * to which it belongs. Set this to <code>null</code> if this record belongs to
     * a single-record entity or if it was the initial record of the first
     * multi-record entity to which it belonged (even if it later re-resolved into a
     * larger entity).
     * 
     * @param principle The optional principle describing why the record merged into
     *                  the entity to which it belongs, or <code>null</code> if this
     *                  record belongs to a single-record entity or if it was the
     *                  initial record of the first multi-record entity to which it
     *                  belonged.
     */
    public void setPrinciple(String principle) {
        this.principle = principle;
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "dataSource=[ " + this.getDataSource() 
                + " ], recordId=[ " + this.getRecordId() 
                + " ], matchKey=[ " + this.getMatchKey() 
                + " ], principle=[ " + this.getPrinciple() + " ]";
    }
}
