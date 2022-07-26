package com.senzing.datamart.model;

import com.senzing.util.JsonUtilities;

import javax.json.*;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;

/**
 * Provides a base class to describe an entity comprised of records.
 */
public abstract class SzEntity {
  /**
   * The entity ID for this entity.
   */
  private long entityId;

  /**
   * The entity name for the entity.
   */
  private String entityName;

  /**
   * The data sources from the associated records.
   */
  private Map<String, Integer> sourceSummary;

  /**
   * The associated records.
   */
  private Set<SzRecord> records;

  /**
   * Default constructor.
   */
  protected SzEntity() {
    this.sourceSummary = new LinkedHashMap<>();
    this.records       = new LinkedHashSet<>();
  }

  /**
   * Gets the entity ID for this instance.
   *
   * @return The entity ID for this instance.
   */
  public long getEntityId() {
    return entityId;
  }

  /**
   * Sets the entity ID for this instance.
   *
   * @param entityId The entity ID for this instance.
   */
  public void setEntityId(long entityId) {
    this.entityId = entityId;
  }

  /**
   * Gets the entity name for the entity.
   *
   * @return The entity name for the entity.
   */
  public String getEntityName() {
    return entityName;
  }

  /**
   * Sets the entity name for the entity.
   *
   * @param entityName The name for the entity.
   */
  public void setEntityName(String entityName) {
    this.entityName = entityName;
  }

  /**
   * Gets the associated data sources and the number of records for each
   * data source as an <b>unmodifiable</b> {@link Map} of {@link String}
   * data source codes to {@link Integer} values.
   *
   * @return The <b>unmodifiable</b> {@link Map} of data source keys to
   *         {@link Integer} values representing the number of records for that
   *         data source.
   */
  public Map<String, Integer> getSourceSummary() {
    return Collections.unmodifiableMap(this.sourceSummary);
  }

  /**
   * Gets the associated records as an <b>unmodifiable</b> {@link List} of
   * {@link SzRecord} instances.
   *
   * @return The <b>unmodifiable</b> {@link List} of {@link SzRecord} instances.
   */
  public Set<SzRecord> getRecords() {
    return Collections.unmodifiableSet(this.records);
  }

  /**
   * Sets the associated records to those in the specified {@link
   * Collection} of {@link SzRecord} instances.
   *
   * @param records The {@link Collection} of {@link SzRecord} instances
   *                describing the records to associate with this entity.
   */
  public void setRecords(Collection<SzRecord> records) {
    this.records.clear();
    this.sourceSummary.clear();
    this.records.addAll(records);
    this.records.forEach(record -> {
      this.addRecord(record);
    });
  }

  /**
   * Adds the record described by the specified {@link SzRecord} to this entity.
   *
   * @param record The {@link SzRecord} describing the record to add.
   */
  public void addRecord(SzRecord record) {
    String dataSource = record.getDataSource();
    Integer count = this.sourceSummary.get(dataSource);
    if (count == null) {
      this.sourceSummary.put(dataSource, 1);
    } else {
      this.sourceSummary.put(dataSource, count + 1);
    }
    this.records.add(record);
  }

  /**
   * Removes all associated records from this entity.
   */
  public void clearRecords() {
    this.sourceSummary.clear();
    this.records.clear();
  }

  /**
   * Populates the specified {@link JsonObjectBuilder} with the properties
   * of this instance.
   *
   * @param builder The {@link JsonObjectBuilder} to populate.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("id", this.getEntityId());
    builder.add("name", this.getEntityName());
    JsonArrayBuilder jab = Json.createArrayBuilder();
    Set<SzRecord> records = this.getRecords();
    SortedSet<SzRecord> sortedRecords = new TreeSet<>(records);
    for (SzRecord record : sortedRecords) {
      JsonObjectBuilder job = Json.createObjectBuilder();
      record.buildJson(job);
      jab.add(job);
    }
    builder.add("records", jab);
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
   * Parses the specified JSON and populates the specified {@link SzEntity}.
   *
   * @param entity The {@link SzEntity} to populate.
   * @param jsonObject The {@link JsonObject} describing the entity.
   */
  public static void parse(SzEntity entity, JsonObject jsonObject) {
    Long entityId = getLong(jsonObject, "id");
    if (entityId != null) {
      entityId = getLong(jsonObject, "ENTITY_ID");
    }

    String entityName = getString(jsonObject, "name");
    if (entityName == null) {
      entityName = getString(jsonObject, "ENTITY_NAME");
    }
    entity.setEntityName(entityName);

    JsonArray recordArray = getJsonArray(jsonObject, "records");
    if (recordArray == null) {
      recordArray = getJsonArray(jsonObject, "RECORDS");
    }

    List<SzRecord> records = new ArrayList<>(recordArray.size());
    for (JsonObject jsonObj : recordArray.getValuesAs(JsonObject.class)) {
      SzRecord record = SzRecord.parse(jsonObj);
      records.add(record);
    }

    entity.setRecords(records);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntity that = (SzEntity) o;
    return this.getEntityId() == that.getEntityId()
        && Objects.equals(this.getEntityName(), that.getEntityName())
        && this.getSourceSummary().equals(that.getSourceSummary())
        && this.getRecords().equals(that.getRecords());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEntityId(),
                        this.getEntityName(),
                        this.getSourceSummary(),
                        this.getRecords());
  }

  @Override
  public String toString() {
    return this.toJsonText();
  }
}
