package com.senzing.datamart.model;

import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Encapsulates the identifying information for a record that is replicated to
 * the data mart.
 */
public class SzRecordKey implements Comparable<SzRecordKey> {
    /**
     * The data source code for the record.
     */
    private String dataSource;

    /**
     * The record ID for the record.
     */
    private String recordId;

    /**
     * Constructs with the specified data source and record ID.
     * 
     * @param dataSource The data source code for the data source.
     * @param recordId   The record ID for the record.
     */
    public SzRecordKey(String dataSource, String recordId) {
        this.dataSource = dataSource;
        this.recordId = recordId;
    }

    /**
     * Gets the data source code for the record.
     *
     * @return The data source code for the record.
     */
    public String getDataSource() {
        return this.dataSource;
    }

    /**
     * Gets the record ID for the record.
     *
     * @return The record ID for the record.
     */
    public String getRecordId() {
        return this.recordId;
    }

    /**
     * Overridden to return <code>true</code> if and only if the specified parameter
     * is an instance of the same class with equivalent properties.
     * 
     * @param o The object to compare with.
     * @return <code>true</code> if the specified parameter is an instance of the 
     *         same class with equivalent properties, otherwise <code>false</code>.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SzRecordKey that = (SzRecordKey) o;
        return Objects.equals(this.getDataSource(), that.getDataSource())
                && Objects.equals(this.getRecordId(), that.getRecordId());
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)} 
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getDataSource(), this.getRecordId());
    }

    /**
     * Implemented to order {@link SzRecordKey} instances first by data source code
     * and then by record ID. A <code>null</code> reference compares less-than a
     * non-null reference.
     *
     * @param record The record to compare to.
     * @return A negative number if this instance is less-than the specified
     *         {@link SzRecordKey}, a positive number if this instance is
     *         greater-than the specified instance, or zero (0) if they are equal.
     */
    public int compareTo(SzRecordKey record) {
        if (record == null) {
            return -1;
        }
        String src1 = this.getDataSource();
        String src2 = record.getDataSource();
        int diff = src1.compareTo(src2);
        if (diff != 0) {
            return diff;
        }
        String id1 = this.getRecordId();
        String id2 = record.getRecordId();
        diff = id1.compareTo(id2);
        return diff;
    }

    /**
     * Populates the specified {@link JsonObjectBuilder} with the properties of this
     * instance.
     *
     * @param builder The {@link JsonObjectBuilder} to populate.
     */
    public void buildJson(JsonObjectBuilder builder) {
        builder.add("src", this.getDataSource());
        builder.add("id", this.getRecordId());
    }

    /**
     * Converts this instance to a {@link JsonObject}.
     *
     * @return This instance as a {@link JsonObject}.
     */
    public JsonObject toJsonObject() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        this.buildJson(job);
        return job.build();
    }

    /**
     * Converts this instance to JSON text, optionally pretty printing.
     *
     * @param prettyPrint <code>true</code> if the JSON should be pretty printed,
     *                    otherwise <code>false</code>.
     *
     * @return The JSON text for this instance.
     */
    public String toJsonText(boolean prettyPrint) {
        return JsonUtilities.toJsonText(this.toJsonObject(), prettyPrint);
    }

    /**
     * Converts this instance to JSON text.
     *
     * @return The JSON text for this instance.
     */
    public String toJsonText() {
        return this.toJsonText(false);
    }

    /**
     * Parses the specified JSON as an {@link SzRecordKey} instance.
     *
     * @param jsonObject The {@link JsonObject} describing the record.
     * @return The {@link SzRecordKey} describing the record.
     */
    public static SzRecordKey parse(JsonObject jsonObject) {
        String src = JsonUtilities.getString(jsonObject, "src");
        if (src == null) {
            src = JsonUtilities.getString(jsonObject, "dataSourceCode");
        }
        if (src == null) {
            src = JsonUtilities.getString(jsonObject, "DATA_SOURCE");
        }
        String id = JsonUtilities.getString(jsonObject, "id");
        if (id == null) {
            id = JsonUtilities.getString(jsonObject, "recordId");
        }
        if (id == null) {
            id = JsonUtilities.getString(jsonObject, "RECORD_ID");
        }
        if (src == null || id == null) {
            throw new IllegalArgumentException(
                "The specified JsonObject does not have the required fields.  src=[ "
                + src + " ], id=[ " + id + " ], jsonObject=[ " + jsonObject + " ]");
        }

        // set the properties
        return new SzRecordKey(src, id);
    }

    /**
     * Implemented to return the result from {@link #toJsonText()}.
     * 
     * @return The result from {@link #toJsonText()}.
     */
    @Override
    public String toString() {
        return this.toJsonText();
    }

    /**
     * Converts this instance to an instance of {@link com.senzing.sdk.SzRecordKey}.
     * 
     * @return The equivalent instance of {@link com.senzing.sdk.SzRecordKey}.
     */
    public com.senzing.sdk.SzRecordKey toKey() {
        return com.senzing.sdk.SzRecordKey.of(this.getDataSource(), 
                                              this.getRecordId());
    }
}
