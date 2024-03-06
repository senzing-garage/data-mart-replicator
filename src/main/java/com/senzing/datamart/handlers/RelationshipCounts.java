package com.senzing.datamart.handlers;

/**
 * Handles counting relationships given two data source summaries.
 */
public class RelationshipCounts {
    /**
     * The first data source.
     */
    private String source1;

    /**
     * The second data source.
     */
    private String source2;

    /**
     * The number of records of the "from" data source related to records of the
     * second data source -- counting both directions if applicable.
     */
    private int recordCount = 0;

    /**
     * Constructs with the specified data sources.
     *
     * @param source1 The first data source.
     * @param source2 The second data soure.
     */
    public RelationshipCounts(String source1, String source2) {
        this.source1 = source1;
        this.source2 = source2;
        this.recordCount = 0;
    }

    /**
     * Gets the first data source.
     *
     * @return The first data source.
     */
    public String getSource1() {
        return this.source1;
    }

    /**
     * Gets the second data source.
     *
     * @return The second data source.
     */
    public String getSource2() {
        return this.source2;
    }

    /**
     * Gets the count of records of the first data source to entities having records
     * from the second data source. If the data sources differ and they are present
     * on both sides of the relationship then this the sum of records from both
     * sides.
     *
     * @return The count of records of the first data source to entities having
     *         records from the second data source.
     */
    public int getRecordCount() {
        return this.recordCount;
    }

    /**
     * Sets the count of records of the first data source to entities having records
     * from the second data source. If the data sources differ and they are present
     * on both sides of the relationship then this the sum of records from both
     * sides.
     *
     * @param count The count of records of the first data source to entities having
     *              records from the second data source.
     */
    public void setRecordCount(int count) {
        this.recordCount = count;
    }

    /**
     * Increment the count of records of the first data source to entities having
     * records from the second data source. If the data sources differ and they are
     * present on both sides of the relationship then this the sum of records from
     * both sides.
     *
     * @param delta The delta by which to increment the record count.
     */
    public void incrementRecordCount(int delta) {
        this.recordCount += delta;
    }

    @Override
    public String toString() {
        return "source1=[ " + this.getSource1() + " ], source2=[ " + this.getSource2() + " ], recordCount=[ "
                + this.getRecordCount() + " ]";
    }
}
