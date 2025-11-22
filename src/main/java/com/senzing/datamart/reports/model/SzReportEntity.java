package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.sdk.SzRecordKey;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

/**
 * Describes an entity from the data mart.
 */
public class SzReportEntity implements Serializable {
    /**
     * The entity ID that uniquely identifies the entity.
     */
    private long entityId = 0L;

    /**
     * The best name for the entity.
     */
    private String entityName = null;

    /**
     * The number of records resolved to this entity.
     */
    private Integer recordCount = null;

    /**
     * The number of entities that are related to this entity.
     */
    private Integer relationCount = null;

    /**
     * The {@link Map} of {@link Long} entity ID's identifying the entities on this
     * page.
     */
    private SortedMap<SzRecordKey, SzReportRecord> records = null;

    /**
     * Default constructor
     */
    public SzReportEntity() {
        this(0L);
    }

    /**
     * Constructs with the specified entity ID.
     * 
     * @param entityId The entity ID for the entity.
     */
    public SzReportEntity(long entityId) {
        this(entityId, null);
    }

    /**
     * Constructs with the specified entity ID.
     * 
     * @param entityId   The entity ID for the entity.
     * @param entityName The best name for the entity.
     */
    public SzReportEntity(long entityId, String entityName) {
        this.entityId = entityId;
        this.entityName = entityName;
        this.recordCount = null;
        this.relationCount = null;
        this.records = new TreeMap<>();
    }

    /**
     * Gets the entity ID that uniquely identifies the entity.
     *
     * @return The entity ID that uniquely identifies the entity.
     */
    public long getEntityId() {
        return this.entityId;
    }

    /**
     * Sets the entity ID that uniquely identifies the entity.
     *
     * @param entityId The entity ID that uniquely identifies the entity.
     */
    public void setEntityId(long entityId) {
        this.entityId = entityId;
    }

    /**
     * Gets the best name associated with the entity.
     * 
     * @return The best name associated with the entity.
     */
    @JsonInclude(NON_EMPTY)
    public String getEntityName() {
        return this.entityName;
    }

    /**
     * Sets the best name associated with the entity.
     * 
     * @param name The best name associated with the entity.
     */
    public void setEntityName(String name) {
        this.entityName = name;
    }

    /**
     * Gets the number of records resolved to this entity.
     * 
     * @return The number of records resolved to this entity.
     */
    @JsonInclude(NON_NULL)
    public Integer getRecordCount() {
        return this.recordCount;
    }

    /**
     * Sets the number of records that are resolved to this entity.
     * 
     * @param recordCount The number of records that are resolved to this entity.
     */
    public void setRecordCount(Integer recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Gets the number of entities that are related to this entity.
     * 
     * @return The number of entities that are related to this entity.
     */
    @JsonInclude(NON_NULL)
    public Integer getRelationCount() {
        return this.relationCount;
    }

    /**
     * Sets the number of entities that are related to this entity.
     * 
     * @param relationCount The number of entities that are related to this entity.
     */
    public void setRelationCount(Integer relationCount) {
        this.relationCount = relationCount;
    }

    /**
     * Gets the {@link List} of {@link SzReportRecord} instances describing the records in
     * the entity.
     * 
     * @return The {@link List} of {@link SzReportRecord} instances describing the records
     *         in the entity.
     */
    @JsonInclude(NON_EMPTY)
    public List<SzReportRecord> getRecords() {
        List<SzReportRecord> records = new ArrayList<>(this.records.values());
        return records;
    }

    /**
     * Sets the {@link List} of {@link SzReportRecord} instances describing the records in
     * this entity. If two records exist in the {@link Collection} with the same
     * data source code and record ID then the later one replaces the earlier one.
     * 
     * @param records The {@link Collection} of {@link SzReportRecord} instances
     *                describing the records in this entity.
     */
    public void setRecords(Collection<SzReportRecord> records) {
        this.records.clear();
        if (records != null) {
            records.forEach(record -> {
                if (record != null) {
                    SzRecordKey key = SzRecordKey.of(record.getDataSource(), 
                                                     record.getRecordId());
                    this.records.put(key, record);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzReportRecord} to the list of records for this entity.
     * If a record already exists for this entity with the same data source code and
     * record ID, then the specified record replaces the existing one.
     * 
     * @param record The {@link SzReportRecord} describing the record to be added.
     */
    public void addRecord(SzReportRecord record) {
        SzRecordKey key = SzRecordKey.of(record.getDataSource(), record.getRecordId());
        this.records.put(key, record);
    }

    /**
     * Removes any record from this entity with the specified data source code and
     * record ID. If there is no such record in the entity then this method has no
     * effect.
     * 
     * @param dataSourceCode The data source code for the record to remove.
     * @param recordId       The record ID for the record to remove.
     */
    public void removeRecord(String dataSourceCode, String recordId) {
        SzRecordKey key = SzRecordKey.of(dataSourceCode, recordId);
        this.records.remove(key);
    }

    /**
     * Removes all records for this entity.
     */
    public void removeAllRecords() {
        this.records.clear();
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "entityId=[ " + this.getEntityId() 
                + " ], entityName=[ " + this.getEntityName()
                + " ], recordCount=[ " + this.getRecordCount()
                + " ], relationCount=[ " + this.getRelationCount()
                + " ], records=[ " + this.getRecords() + " ]";
    }
}
