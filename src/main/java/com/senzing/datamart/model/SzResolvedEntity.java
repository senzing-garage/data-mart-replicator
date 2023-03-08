package com.senzing.datamart.model;


import com.senzing.util.JsonUtilities;
import com.senzing.util.LoggingUtilities;
import com.senzing.util.ZipUtilities;

import javax.json.*;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;

/**
 * Encapsulates the information for an entity that is replicated for the data
 * mart.
 */
public class SzResolvedEntity extends SzEntity {
  /**
   * The {@link Map} of {@link Long} entity ID keys to {@link SzRelatedEntity}
   * values describing the related entities.
   */
  public Map<Long, SzRelatedEntity> relatedEntities;

  /**
   * Default constructor.
   */
  public SzResolvedEntity() {
    this.relatedEntities = new LinkedHashMap<>();
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Map} of {@link Long} entity ID keys to
   * {@link SzRelatedEntity} values describing the related entities.
   *
   * @return The <b>unmodifiable</b> {@link Map} of {@link Long} entity ID keys
   *         to {@link SzRelatedEntity} values describing the related entities.
   */
  public Map<Long, SzRelatedEntity> getRelatedEntities() {
    return Collections.unmodifiableMap(this.relatedEntities);
  }

  /**
   * Sets the related entities for this entity to those described in the
   * specified {@link Collection} of {@link SzRelatedEntity} instances.
   *
   * @param entities The {@link Collection} of {@link SzRelatedEntity}
   *                 instances.
   */
  public void setRelatedEntities(Collection<SzRelatedEntity> entities) {
    long entityId = this.getEntityId();
    for (SzRelatedEntity entity: entities) {
      if (entity.getEntityId() == entityId) {
        throw new IllegalArgumentException(
            "None of the specified related entities can have the same entity "
                + "ID as this instance: " + entity);
      }
    }
    this.relatedEntities.clear();
    entities.forEach(entity -> {
      this.relatedEntities.put(entity.getEntityId(), entity);
    });
  }

  /**
   * Adds the related entity described by the specified {@link SzRelatedEntity}
   * instance to the related entities for this instance.
   *
   * @param entity The {@link SzRelatedEntity} describing the related entity to
   *               the related entities for this instance.
   */
  public void addRelatedEntity(SzRelatedEntity entity) {
    if (entity.getEntityId() == this.getEntityId()) {
      throw new IllegalArgumentException(
          "A related entity cannot have the same entity ID as this entity: "
          + entity);
    }
    this.relatedEntities.put(entity.getEntityId(), entity);
  }

  /**
   * Removes all related entities from this entity.
   */
  public void clearRelatedEntities() {
    this.relatedEntities.clear();
  }

  /**
   * Populates the specified {@link JsonObjectBuilder} with the properties
   * of this instance.
   *
   * @param builder The {@link JsonObjectBuilder} to populate.
   */
  public void buildJson(JsonObjectBuilder builder) {
    super.buildJson(builder);
    JsonArrayBuilder jab = Json.createArrayBuilder();
    Map<Long, SzRelatedEntity> relations = this.getRelatedEntities();
    SortedMap<Long, SzRelatedEntity> sortedRelations
        = new TreeMap<>(relations);

    Collection<SzRelatedEntity> relatedEntities
        = sortedRelations.values();

    for (SzRelatedEntity related : relatedEntities) {
      JsonObjectBuilder job = Json.createObjectBuilder();
      related.buildJson(job);
      jab.add(job);
    }
    builder.add("related", jab);
  }

  /**
   * Parses the specified entity hash text and returns an {@link
   * SzResolvedEntity} describing the entity.
   *
   * @param hashText The entity hash text describing the entity.
   * @return The {@link SzResolvedEntity} that was populated.
   *
   */
  public static SzResolvedEntity parseHash(String hashText) {
    if (hashText == null) return null;
    String jsonText = ZipUtilities.unzipText64(hashText);
    try {
      return parse(jsonText);
    } catch (RuntimeException e) {
      System.err.println(jsonText);
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Parses the specified JSON text and returns an {@link SzResolvedEntity}
   * describing the entity.
   *
   * @param jsonText The JSON text describing the entity.
   * @return The {@link SzResolvedEntity} that was populated.
   *
   */
  public static SzResolvedEntity parse(String jsonText) {
    if (jsonText == null) return null;
    JsonObject jsonObject = parseJsonObject(jsonText);
    return parse(jsonObject);
  }

  /**
   * Parses the specified JSON and returns an {@link SzResolvedEntity} describing the
   * entity.
   *
   * @param jsonObject The {@link JsonObject} describing the entity.
   * @return The {@link SzResolvedEntity} that was populated.
   */
  public static SzResolvedEntity parse(JsonObject jsonObject) {
    if (jsonObject == null) return null;
    SzResolvedEntity entity = new SzResolvedEntity();

    // handle the base data
    JsonObject resolvedObj = jsonObject;
    if (jsonObject.containsKey("RESOLVED_ENTITY")) {
      resolvedObj = getJsonObject(jsonObject, "RESOLVED_ENTITY");
    }
    SzEntity.parse(entity, resolvedObj);

    // now find the related entities
    JsonArray relatedArray = getJsonArray(jsonObject, "related");
    if (relatedArray == null) {
      relatedArray = getJsonArray(jsonObject, "RELATED_ENTITIES");
    }

    List<SzRelatedEntity> relatedEntities
        = new ArrayList<>(relatedArray == null ? 0 : relatedArray.size());

    if (relatedArray != null) {
      for (JsonObject jsonObj : relatedArray.getValuesAs(JsonObject.class)) {
        SzRelatedEntity related = SzRelatedEntity.parse(null, jsonObj);
        relatedEntities.add(related);
      }
    }

    entity.setRelatedEntities(relatedEntities);

    // return the entity
    return entity;
  }

  /**
   * Generates the hash for this entity.
   *
   * @return The hash for this entity.
   */
  public String toHash() {
    String jsonText = this.toJsonText();
    return ZipUtilities.zipText64(jsonText);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SzResolvedEntity that = (SzResolvedEntity) o;
    return this.getRelatedEntities().equals(that.getRelatedEntities());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.getRelatedEntities());
  }
}
