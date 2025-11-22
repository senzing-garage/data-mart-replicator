package com.senzing.datamart.model;

import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Encapsulates the information for the record that is replicated for the data
 * mart.
 */
public class SzRecord {
    /**
     * The {@link SzRecordKey} describing the data source code and record ID pair
     * that identify this record.
     */
    private SzRecordKey recordKey;

    /**
     * The match key that bound this record to its respective entity, or
     * <code>null</code> if this is the first record in that entity.
     */
    private String matchKey;

    /**
     * The principle that bound this record to its respective entity, or
     * <code>null</code> if this is the first record in that entity.
     */
    private String principle;

    /**
     * Constructs with the specified data source, record ID, optional match key and
     * optional principle.
     * 
     * @param dataSource The data source code for the data source.
     * @param recordId   The record ID for the record.
     * @param matchKey   The match key that bound this record to its entity or
     *                   <code>null</code> if this was the first record in the
     *                   entity.
     * @param principle  The principle that bound this record to its entity or
     *                   <code>null</code> if this was the first record in the
     *                   entity.
     * 
     * @throws NullPointerException If the data source or record ID are
     *                              <code>null</code>.
     */
    public SzRecord(String dataSource, String recordId, String matchKey, String principle) {
        this(new SzRecordKey(dataSource, recordId), matchKey, principle);
    }

    /**
     * Constructs with the specified parameters.
     * 
     * @param recordKey The {@link SzRecordKey} for the record.
     * @param matchKey  The match key that binds the record to the entity.
     * @param principle The principle that bound the record to the entity.
     */
    public SzRecord(SzRecordKey recordKey, String matchKey, String principle) {
        Objects.requireNonNull(recordKey, "The record key cannot be null");

        // normalize the match key and principle
        matchKey = (matchKey == null) ? null : matchKey.trim();
        principle = (principle == null) ? null : principle.trim();
        if (matchKey != null && matchKey.length() == 0) {
            matchKey = null;
        }
        if (principle != null && principle.length() == 0) {
            principle = null;
        }

        // set the fields
        this.recordKey = recordKey;
        this.matchKey = matchKey;
        this.principle = principle;
    }

    /**
     * Gets the data source code for the record. This is a convenience function for
     * calling {@link SzRecordKey#getDataSource()} on the result from
     * {@link #getRecordKey()}.
     *
     * @return The data source code for the record.
     * 
     * @see #getRecordKey()
     * @see SzRecordKey#getDataSource()
     */
    public String getDataSource() {
        return this.getRecordKey().getDataSource();
    }

    /**
     * Gets the record ID for the record. This is a convenience function for calling
     * {@link SzRecordKey#getRecordId()} on the result from {@link #getRecordKey()}.
     *
     * @return The record ID for the record.
     * 
     * @see #getRecordKey()
     * @see SzRecordKey#getRecordId()
     */
    public String getRecordId() {
        return this.getRecordKey().getRecordId();
    }

    /**
     * Gets the {@link SzRecordKey} containing the data source code and record ID
     * pair that identify this record.
     * 
     * @return The {@link SzRecordKey} containing the data source code and record ID
     *         pair that identify this record.
     */
    public SzRecordKey getRecordKey() {
        return this.recordKey;
    }

    /**
     * Gets the match key that bound this record to its respective entity. This
     * returns <code>null</code> if this is the first record in that entity.
     * 
     * @return The match key that bound this record to its its respective entity, or
     *         <code>null</code> if this is the first record in that entity.
     */
    public String getMatchKey() {
        return this.matchKey;
    }

    /**
     * Gets the principle that bound this record to its respective entity. This
     * returns <code>null</code> if this is the first record in that entity.
     * 
     * @return The principle that bound this record to its its respective entity, or
     *         <code>null</code> if this is the first record in that entity.
     */
    public String getPrinciple() {
        return this.principle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SzRecord that = (SzRecord) o;
        return Objects.equals(this.getRecordKey(), that.getRecordKey())
                && Objects.equals(this.getMatchKey(), that.getMatchKey())
                && Objects.equals(this.getPrinciple(), that.getPrinciple());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getRecordKey(), this.getMatchKey(), this.getPrinciple());
    }

    /**
     * Populates the specified {@link JsonObjectBuilder} with the properties of this
     * instance.
     *
     * @param builder The {@link JsonObjectBuilder} to populate.
     */
    public void buildJson(JsonObjectBuilder builder) {
        // handle the key fields
        this.getRecordKey().buildJson(builder);

        String matchKey = this.getMatchKey();
        if (matchKey != null) {
            builder.add("mkey", matchKey);
        }
        String principle = this.getPrinciple();
        if (principle != null) {
            builder.add("rule", principle);
        }
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
     * Parses the specified JSON as an {@link SzRecord} instance.
     *
     * @param jsonObject The {@link JsonObject} describing the record.
     * @return The {@link SzRecord} describing the record.
     */
    public static SzRecord parse(JsonObject jsonObject) {
        // parse the record key (data source code and record ID)
        SzRecordKey key = SzRecordKey.parse(jsonObject);

        // get the match key
        String mkey = JsonUtilities.getString(jsonObject, "mkey");
        if (mkey == null) {
            mkey = JsonUtilities.getString(jsonObject, "matchKey");
        }
        if (mkey == null) {
            mkey = JsonUtilities.getString(jsonObject, "MATCH_KEY");
        }

        // get the principle
        String rule = JsonUtilities.getString(jsonObject, "rule");
        if (rule == null) {
            rule = JsonUtilities.getString(jsonObject, "principle");
        }
        if (rule == null) {
            rule = JsonUtilities.getString(jsonObject, "ERRULE_CODE");
        }

        // set the properties
        return new SzRecord(key, mkey, rule);
    }

    @Override
    public String toString() {
        return this.toJsonText();
    }
}
