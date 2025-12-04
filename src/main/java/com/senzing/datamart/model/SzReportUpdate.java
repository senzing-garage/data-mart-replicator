package com.senzing.datamart.model;

import java.util.Objects;

/**
 * Describes an update to a report.
 */
public class SzReportUpdate {
    /**
     * The report key for the update.
     */
    private SzReportKey reportKey;

    /**
     * The delta on the entity count.
     */
    private int entityDelta;

    /**
     * The delta on the record count.
     */
    private int recordDelta;

    /**
     * The delta on the entity relationship count.
     */
    private int relationDelta;

    /**
     * The entity ID for the update.
     */
    private long entityId;

    /**
     * The associated related entity ID.
     */
    private Long relatedId;

    /**
     * Constructs with the specified entity ID.
     *
     * @param reportKey The {@link SzReportKey} for the report.
     * @param entityId  The entity ID for the report update.
     */
    public SzReportUpdate(SzReportKey reportKey, long entityId) {
        Objects.requireNonNull(reportKey, "The report key cannot be null");
        this.reportKey = reportKey;
        this.entityId = entityId;
        this.relatedId = null;
    }

    /**
     * Constructs with the specified entity ID and related entity ID.
     *
     * @param reportKey The {@link SzReportKey} for the report.
     * @param entityId  The entity ID for the update.
     * @param relatedId The related entity ID for the update.
     */
    public SzReportUpdate(SzReportKey reportKey, long entityId, long relatedId) {
        Objects.requireNonNull(reportKey, "The report key cannot be null");
        this.reportKey = reportKey;
        this.entityId = entityId;
        this.relatedId = relatedId;
    }

    /**
     * Constructs with the specified entity ID.
     *
     * @param reportCode The {@link SzReportCode} for the report.
     * @param statistic  The statistic to be updated.
     * @param entityId   The entity ID for the report update.
     */
    public SzReportUpdate(SzReportCode reportCode, String statistic, long entityId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        this.reportKey = new SzReportKey(reportCode, statistic);
        this.entityId = entityId;
        this.relatedId = null;
    }

    /**
     * Constructs with the specified report key parameters and entity ID.
     *
     * @param reportCode  The {@link SzReportCode} for the report.
     * @param statistic   The statistic to be updated.
     * @param dataSource1 The first data source in the report.
     * @param dataSource2 The second data source in the report.
     * @param entityId    The entity ID for the update.
     */
    public SzReportUpdate(SzReportCode reportCode, String statistic, String dataSource1, String dataSource2, long entityId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        this.reportKey = new SzReportKey(reportCode, statistic, dataSource1, dataSource2);
        this.entityId = entityId;
        this.relatedId = null;
    }

    /**
     * Constructs with the specified report key parameters, entity ID and related
     * entity ID.
     *
     * @param reportCode The {@link SzReportCode} for the report.
     * @param statistic  The statistic to be updated.
     * @param entityId   The entity ID for the update.
     * @param relatedId  The related entity ID for the update.
     */
    public SzReportUpdate(SzReportCode reportCode, String statistic, long entityId, long relatedId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        this.reportKey = new SzReportKey(reportCode, statistic);
        this.entityId = entityId;
        this.relatedId = relatedId;
    }

    /**
     * Constructs with the specified report key parameters, entity ID and related
     * entity ID.
     *
     * @param reportCode  The {@link SzReportCode} for the report.
     * @param statistic   The statistic to be updated.
     * @param dataSource1 The first data source in the report.
     * @param dataSource2 The second data source in the report.
     * @param entityId    The entity ID for the update.
     * @param relatedId   The related entity ID for the update.
     */
    public SzReportUpdate(SzReportCode reportCode, String statistic, String dataSource1, String dataSource2, long entityId, long relatedId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        this.reportKey = new SzReportKey(reportCode, statistic, dataSource1, dataSource2);
        this.entityId = entityId;
        this.relatedId = relatedId;
    }

    /**
     * Private constructor for use by the builders.
     *
     * @param reportKey The {@link SzReportKey} for the report.
     * @param entityId  The entity ID for the update.
     * @param relatedId The related entity ID for the update, or <code>null</code>
     *                  if none.
     */
    private SzReportUpdate(SzReportKey reportKey, long entityId, Long relatedId) {
        Objects.requireNonNull(reportKey, "The report key cannot be null");
        this.reportKey = reportKey;
        this.entityId = entityId;
        this.relatedId = relatedId;
    }

    /**
     * Gets the {@link SzReportKey} for this instance.
     *
     * @return The {@link SzReportKey} for this instance.
     */
    public SzReportKey getReportKey() {
        return this.reportKey;
    }

    /**
     * Gets the entity ID for this instance.
     *
     * @return The entity ID for this instance.
     */
    public long getEntityId() {
        return this.entityId;
    }

    /**
     * Gets the entity ID for this instance. This returns <code>null</code> if there
     * is no related entity ID.
     *
     * @return The entity ID for this instance, or <code>null</code> if there is no
     *         related entity ID.
     */
    public Long getRelatedEntityId() {
        return this.relatedId;
    }

    /**
     * Gets the delta on the entity count.
     *
     * @return The delta on the entity count.
     */
    public int getEntityDelta() {
        return this.entityDelta;
    }

    /**
     * Sets the delta on the entity count.
     *
     * @param delta The delta on the entity count.
     */
    public void setEntityDelta(int delta) {
        this.entityDelta = delta;
    }

    /**
     * Gets the delta on the record count.
     *
     * @return The delta on the record count.
     */
    public int getRecordDelta() {
        return this.recordDelta;
    }

    /**
     * Sets the delta on the record count.
     *
     * @param delta The delta on the record count.
     */
    public void setRecordDelta(int delta) {
        this.recordDelta = delta;
    }

    /**
     * Gets the delta on the entity relationship count.
     *
     * @return The delta on the entity relationship count.
     */
    public int getRelationDelta() {
        return this.relationDelta;
    }

    /**
     * Sets the delta on the entity relationship count.
     *
     * @param delta The delta on entity relationship count.
     */
    public void setRelationDelta(int delta) {
        this.relationDelta = delta;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SzReportUpdate{ " + "reportKey=[ " + this.getReportKey() + " ], entityDelta=[ " + this.getEntityDelta()
                + " ], recordDelta=[ " + this.getRecordDelta() + " ], relationDelta=[ " + this.getRelationDelta()
                + " ], entityId=[ " + this.getEntityId() + " ], relatedId=[ " + this.getRelatedEntityId() + " ] }";
    }

    /**
     * Constructs with the specified entity ID.
     *
     * @param reportKey The {@link SzReportKey} for the report.
     * @param entityId  The entity ID for the report update.
     * 
     * @return A new {@link Builder} instance using the specified parameters.
     */
    public static Builder builder(SzReportKey reportKey, long entityId) {
        Objects.requireNonNull(reportKey, "The report key cannot be null");
        return new Builder(reportKey, entityId, null);
    }

    /**
     * Constructs with the specified {@link SzReportKey}, entity ID and related
     * entity ID.
     *
     * @param reportKey The {@link SzReportKey} for the report.
     * @param entityId  The entity ID for the update.
     * @param relatedId The related entity ID for the update.
     * @return The {@link Builder} that was created.
     */
    public static Builder builder(SzReportKey reportKey, long entityId, long relatedId) {
        Objects.requireNonNull(reportKey, "The report key cannot be null");
        return new Builder(reportKey, entityId, relatedId);
    }

    /**
     * Constructs with the specified entity ID.
     *
     * @param reportCode The {@link SzReportCode} for the report.
     * @param statistic  The statistic to be updated.
     * @param entityId   The entity ID for the report update.
     * @return The {@link Builder} that was created.
     */
    public static Builder builder(SzReportCode reportCode, Object statistic, long entityId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        return new Builder(new SzReportKey(reportCode, statistic.toString()), entityId, null);
    }

    /**
     * Constructs with the specified report key parameters and entity ID.
     *
     * @param reportCode  The {@link SzReportCode} for the report.
     * @param statistic   The statistic to be updated.
     * @param dataSource1 The first data source in the report.
     * @param dataSource2 The second data source in the report.
     * @param entityId    The entity ID for the update.
     * @return The {@link Builder} that was created.
     */
    public static Builder builder(SzReportCode reportCode, Object statistic, String dataSource1, String dataSource2, long entityId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");

        SzReportKey reportKey = new SzReportKey(reportCode, statistic.toString(), dataSource1, dataSource2);

        return new Builder(reportKey, entityId, null);
    }

    /**
     * Constructs with the specified report key parameters, entity ID and related
     * entity ID.
     *
     * @param reportCode The {@link SzReportCode} for the report.
     * @param statistic  The statistic to be updated.
     * @param entityId   The entity ID for the update.
     * @param relatedId  The related entity ID for the update.
     * @return The {@link Builder} that was created.
     */
    public static Builder builder(SzReportCode reportCode, Object statistic, long entityId, long relatedId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        return new Builder(new SzReportKey(reportCode, statistic.toString()), entityId, relatedId);
    }

    /**
     * Constructs with the specified report key parameters, entity ID and related
     * entity ID.
     *
     * @param reportCode  The {@link SzReportCode} for the report.
     * @param statistic   The statistic to be updated.
     * @param dataSource1 The first data source in the report.
     * @param dataSource2 The second data source in the report.
     * @param entityId    The entity ID for the update.
     * @param relatedId   The related entity ID for the update.
     * @return The {@link Builder} that was created.
     */
    public static Builder builder(SzReportCode reportCode, Object statistic, String dataSource1, String dataSource2, long entityId, long relatedId) {
        Objects.requireNonNull(reportCode, "The report code cannot be null");
        Objects.requireNonNull(statistic, "The statistic cannot be null");
        SzReportKey reportKey = new SzReportKey(reportCode, statistic.toString(), dataSource1, dataSource2);

        return new Builder(reportKey, entityId, relatedId);
    }

    /**
     * Provides a {@link Builder} for {@link SzReportUpdate} instances.
     */
    public static final class Builder {
        /**
         * The associated {@link SzReportKey} for this instance.
         */
        private SzReportKey reportKey;

        /**
         * The associated entity ID.
         */
        private long entityId;

        /**
         * The associated related entity ID, or <code>null</code> if none.
         */
        private Long relatedId;

        /**
         * The delta in the entity count.
         */
        private int entityDelta;

        /**
         * The delta in the record count.
         */
        private int recordDelta;

        /**
         * The delta in the related entity count.
         */
        private int relationDelta;

        /**
         * Constructs with the specified parameters.
         *
         * @param reportKey The {@link SzReportKey} for this instance.
         * @param entityId  The entity ID for this instance.
         * @param relatedId The related entity ID, or <code>null</code> if none.
         */
        private Builder(SzReportKey reportKey, long entityId, Long relatedId) {
            this.reportKey = reportKey;
            this.entityId = entityId;
            this.relatedId = relatedId;
            this.entityDelta = 0;
            this.recordDelta = 0;
            this.relationDelta = 0;
        }

        /**
         * Sets the entity count delta.
         *
         * @param delta The delta in the entity count.
         *
         * @return This builder instance.
         */
        public Builder entities(int delta) {
            this.entityDelta = delta;
            return this;
        }

        /**
         * Sets the record count delta.
         *
         * @param delta The delta in the record count.
         *
         * @return This builder instance.
         */
        public Builder records(int delta) {
            this.recordDelta = delta;
            return this;
        }

        /**
         * Sets the entity relationship count delta.
         *
         * @param delta The delta in the entity relationship count.
         *
         * @return This builder instance.
         */
        public Builder relations(int delta) {
            this.relationDelta = delta;
            return this;
        }

        /**
         * Builds the {@link SzReportUpdate} instance.
         *
         * @return The new {@link SzReportUpdate} instance.
         */
        public SzReportUpdate build() {
            SzReportUpdate updater = new SzReportUpdate(this.reportKey, this.entityId, this.relatedId);
            updater.setEntityDelta(this.entityDelta);
            updater.setRecordDelta(this.recordDelta);
            updater.setRelationDelta(this.relationDelta);
            return updater;
        }
    }
}
