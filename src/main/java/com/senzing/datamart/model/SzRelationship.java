package com.senzing.datamart.model;

import com.senzing.util.JsonUtilities;
import com.senzing.util.LoggingUtilities;
import com.senzing.util.ZipUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;

/**
 * Describes a relationship between two entities as it is stored in the
 * data mart.
 */
public class SzRelationship {
  /**
   * The entity ID, which is always the greatest of the two entity ID's.
   */
  private long entityId;

  /**
   * The related entity ID, which is always the greatest of the two
   * entity ID's.
   */
  private long relatedId;

  /**
   * The match level for this related entity.
   */
  private int matchLevel;

  /**
   * The {@link SzMatchType} for this related entity.
   */
  private SzMatchType matchType;

  /**
   * The match key for this related entity.
   */
  private String matchKey;

  /**
   * The principle used for relating the entities.
   */
  private String principle;

  /**
   * The {@link Map} of {@link String} data source keys to {@link Integer}
   * values indicating the number of records having that data source for
   * the entity with the lower entity ID.
   */
  private Map<String, Integer> sourceSummary;

  /**
   * The {@link Map} of {@link String} data source keys to {@link Integer}
   * values indicating the number of records having that data source for
   * the entity with the greater entity ID.
   */
  private Map<String, Integer> relatedSourceSummary;

  /**
   * Constructs with the specified {@link SzResolvedEntity} and {@link
   * SzRelatedEntity} to create the relationship.
   *
   * @param resolvedEntity The {@link SzResolvedEntity} in the relationship.
   * @param relatedEntity The {@link SzRelatedEntity} in the relationship.
   * @throws NullPointerException If either parameter is <code>null</code>.
   * @throws IllegalArgumentException If the specified {@link SzRelatedEntity}
   *                                  is not related to the specified {@link
   *                                  SzResolvedEntity}.
   */
  public SzRelationship(SzResolvedEntity  resolvedEntity,
                        SzRelatedEntity   relatedEntity)
  {
    Objects.requireNonNull(
        resolvedEntity, "The resolved entity cannot be null.");
    Objects.requireNonNull(
        relatedEntity, "The related entity cannot be null.");
    if (!resolvedEntity.getRelatedEntities().containsKey(
        relatedEntity.getEntityId()))
    {
      throw new IllegalArgumentException(
          "The specified related entity is not related to the specified "
              + "resolved entity.  related=[ " + relatedEntity
              + " ], resolved=[ " + resolvedEntity + " ]");
    }
    if (resolvedEntity.getEntityId() == relatedEntity.getEntityId()) {
      throw new IllegalArgumentException(
          "The two entities cannot have the same entity ID: "
              + resolvedEntity.getEntityId());
    }

    this.matchLevel = relatedEntity.getMatchLevel();
    this.matchType  = relatedEntity.getMatchType();
    this.matchKey   = relatedEntity.getMatchKey();
    this.principle  = relatedEntity.getPrinciple();

    boolean flip = (resolvedEntity.getEntityId() > relatedEntity.getEntityId());
    long resolvedEntityId = resolvedEntity.getEntityId();
    long relatedEntityId  = relatedEntity.getEntityId();

    this.entityId   = (flip) ? relatedEntityId : resolvedEntityId;
    this.relatedId  = (flip) ? resolvedEntityId : relatedEntityId;

    this.sourceSummary = (flip) ? relatedEntity.getSourceSummary()
        : resolvedEntity.getSourceSummary();
    this.relatedSourceSummary = (flip) ? resolvedEntity.getSourceSummary()
        : relatedEntity.getSourceSummary();
  }

  /**
   * Constructs with the specified properties.
   *
   * @param entityId1 The first entity ID for the relationship.
   * @param entityId2 The second entity ID for the relationship.
   * @param matchLevel The match level for the relationship.
   * @param matchType The non-null {@link SzMatchType} for the relationship.
   * @param matchKey The non-null match key for the relationship.
   * @param principle The principle (ER Rule Code) for the relationship.
   * @param sourceSummary1 The {@link Map} of {@link String} data source code
   *                       keys to {@link Integer} record counts for the first
   *                       entity ID.
   * @param sourceSummary2 The {@link Map} of {@link String} data source code
   *                       keys to {@link Integer} record counts for the second
   *                       entity ID.
   */
  public SzRelationship(long                  entityId1,
                        long                  entityId2,
                        int                   matchLevel,
                        SzMatchType           matchType,
                        String                matchKey,
                        String                principle,
                        Map<String, Integer>  sourceSummary1,
                        Map<String, Integer>  sourceSummary2)
    throws NullPointerException, IllegalArgumentException
  {
    this(entityId1, entityId2, matchLevel, matchType, matchKey,
         principle, sourceSummary1, sourceSummary2, true);
  }

  /**
   * Private constructor to avoid copying the specified maps.
   *
   * @param entityId1 The first entity ID for the relationship.
   * @param entityId2 The second entity ID for the relationship.
   * @param matchLevel The match level for the relationship.
   * @param matchType The non-null {@link SzMatchType} for the relationship.
   * @param matchKey The non-null match key for the relationship.
   * @param principle The principle (ER Rule Code) for the relationship.
   * @param sourceSummary1 The {@link Map} of {@link String} data source code
   *                       keys to {@link Integer} record counts for the first
   *                       entity ID.
   * @param sourceSummary2 The {@link Map} of {@link String} data source code
   *                       keys to {@link Integer} record counts for the second
   *                       entity ID.
   * @param copyMaps <code>true</code> if the specified maps should be copied,
   *                 or <code>false</code> if the referenced maps should be used
   *                 directly.
   */
  private SzRelationship(long                 entityId1,
                         long                 entityId2,
                         int                  matchLevel,
                         SzMatchType          matchType,
                         String               matchKey,
                         String               principle,
                         Map<String, Integer> sourceSummary1,
                         Map<String, Integer> sourceSummary2,
                         boolean              copyMaps)
      throws NullPointerException, IllegalArgumentException
  {
    Objects.requireNonNull(matchKey, "The match key cannot be null.");
    Objects.requireNonNull(principle, "The principle cannot be null.");
    Objects.requireNonNull(matchType, "The match type cannot be null.");
    Objects.requireNonNull(
        sourceSummary1, "The first source summary cannot be null");
    Objects.requireNonNull(
        sourceSummary2, "The second source summary cannot be null");

    boolean flip = (entityId2 < entityId1);

    this.entityId       = (flip) ? entityId2 : entityId1;
    this.relatedId      = (flip) ? entityId1 : entityId2;
    this.matchLevel     = matchLevel;
    this.matchType      = matchType;
    this.matchKey       = matchKey;
    this.principle      = principle;

    Map<String, Integer> summary1 = (flip) ? sourceSummary2 : sourceSummary1;
    Map<String, Integer> summary2 = (flip) ? sourceSummary1 : sourceSummary2;

    this.sourceSummary
        = (copyMaps) ? new LinkedHashMap<>(summary1) : summary1;

    this.relatedSourceSummary
        = (copyMaps) ? new LinkedHashMap<>(summary2) : summary2;
  }

  /**
   * Gets the lesser of the two entity ID's so the first entity ID is always the
   * lesser of the two for normalizing the relationship properties.
   *
   * @return The lesser of the two entity ID's so the first entity ID is always
   *         the lesser of the two for normalizing the relationship properties.
   */
  public long getEntityId() {
    return this.entityId;
  }

  /**
   * Gets the greater of the two entity ID's so the related ID is always the
   * greater of the two for normalizing the relationship properties.
   *
   * @return The greater of the two entity ID's so the related ID is always the
   *         greater of the two for normalizing the relationship properties.
   */
  public long getRelatedEntityId() {
    return this.relatedId;
  }

  /**
   * Gets the match level for this related entity.
   *
   * @return The match level for this related entity.
   */
  public int getMatchLevel() {
    return this.matchLevel;
  }

  /**
   * Gets the {@link SzMatchType} describing the relationship type for this
   * related entity.
   *
   * @return The relationship type for this related entity.
   */
  public SzMatchType getMatchType() {
    return this.matchType;
  }

  /**
   * Gets the match key for this related entity.
   *
   * @return The match key for this related entity.
   */
  public String getMatchKey() {
    return this.matchKey;
  }

  /**
   * Gets the principle code identifying the Entity Resolution Rule
   * that created this relationship.
   *
   * @return The principle code identifying the Entity Resolution Rule
   *         that created this relationship.
   */
  public String getPrinciple() {
    return this.principle;
  }

  /**
   * Gets the {@link Map} of {@link String} data source code keys to {@link
   * Integer} record count values describing the number of records per data
   * source for the entity in the relationship having the lesser entity ID.
   *
   * @return The {@link Map} of {@link String} data source code keys to {@link
   *         Integer} record count values describing the number of records per
   *         data source for the entity in the relationship having the lesser
   *         entity ID.
   */
  public Map<String, Integer> getSourceSummary() {
    return Collections.unmodifiableMap(this.sourceSummary);
  }

  /**
   * Gets the {@link Map} of {@link String} data source code keys to {@link
   * Integer} record count values describing the number of records per data
   * source for the entity in the relationship having the greater entity ID.
   *
   * @return The {@link Map} of {@link String} data source code keys to {@link
   *         Integer} record count values describing the number of records per
   *         data source for the entity in the relationship having the greater
   *         entity ID.
   */
  public Map<String, Integer> getRelatedSourceSummary() {
    return Collections.unmodifiableMap(this.relatedSourceSummary);
  }


  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || this.getClass() != object.getClass()) return false;
    SzRelationship rel = (SzRelationship) object;
    return getEntityId() == rel.getEntityId()
        && this.getRelatedEntityId() == rel.getRelatedEntityId()
        && this.getMatchLevel() == rel.getMatchLevel()
        && this.getMatchType() == rel.getMatchType()
        && Objects.equals(this.getMatchKey(), rel.getMatchKey())
        && Objects.equals(this.getPrinciple(), rel.getPrinciple())
        && this.getSourceSummary().equals(rel.getSourceSummary())
        && this.getRelatedSourceSummary().equals(rel.getRelatedSourceSummary());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEntityId(),
                        this.getRelatedEntityId(),
                        this.getMatchLevel(),
                        this.getMatchType(),
                        this.getMatchKey(),
                        this.getPrinciple(),
                        this.getSourceSummary(),
                        this.getRelatedSourceSummary());
  }

  /**
   * Adds the JSON properties for this instance to the specified {@link
   * JsonObjectBuilder}.
   *
   * @param builder The {@link JsonObjectBuilder} to which the properties will
   *                be added.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("entityId", this.getEntityId());
    builder.add("relatedId", this.getRelatedEntityId());
    builder.add("matchLevel", this.getMatchLevel());
    builder.add("matchType", this.getMatchType().toString());
    builder.add("matchKey", this.getMatchKey());
    builder.add("principle", this.getPrinciple());
    SortedMap<String, Integer> sortedSummary
        = new TreeMap<>(this.getSourceSummary());
    addProperty(builder, "sourceSummary", sortedSummary);
    SortedMap<String, Integer> sortedRelatedSummary
        = new TreeMap<>(this.getRelatedSourceSummary());
    addProperty(builder, "relatedSummary", sortedRelatedSummary);
  }

  /**
   * Converts this instance to a {@link JsonObject}.
   *
   * @return A {@link JsonObject} describing the properties of this instance.
   */
  public JsonObject toJsonObject() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    this.buildJson(builder);
    return builder.build();
  }

  /**
   * Coverts this instance to JSON text.
   *
   * @return The JSON text describing the properties of this instance.
   */
  public String toJsonText() {
    return JsonUtilities.toJsonText(this.toJsonObject());
  }

  /**
   * Generates the hash for this relationship.
   *
   * @return The hash for this relationship.
   */
  public String toHash() {
    String jsonText = this.toJsonText();
    return ZipUtilities.zipText64(jsonText);
  }

  /**
   * Overridden to return the result from {@link #toJsonText()}.
   *
   * @return The JSON text describing this instance.
   */
  public String toString() {
    return this.toJsonText();
  }

  /**
   * Parses the specified {@link JsonObject} as an instance of this class.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   * @return The newly created {@link SzRelationship}.
   */
  public static SzRelationship parse(JsonObject jsonObject) {
    if (jsonObject == null) return null;

    Long        entityId1   = getLong(jsonObject, "entityId");
    Long        entityId2   = getLong(jsonObject, "relatedId");
    Integer     matchLevel  = getInteger(jsonObject, "matchLevel");
    String      typeString  = getString(jsonObject, "matchType");
    String      matchKey    = getString(jsonObject, "matchKey");
    String      principle   = getString(jsonObject, "principle");
    SzMatchType matchType   = SzMatchType.valueOf(typeString);

    // get the first summary object
    JsonObject summaryObj1
        = getJsonObject(jsonObject, "sourceSummary");
    Map<String, Integer> summary1 = new LinkedHashMap<>();
    for (String key : summaryObj1.keySet()) {
      summary1.put(key, getInteger(summaryObj1, key));
    }

    // get the second summary object
    JsonObject summaryObj2
        = getJsonObject(jsonObject, "relatedSummary");
    Map<String, Integer> summary2 = new LinkedHashMap<>();
    for (String key : summaryObj2.keySet()) {
      summary2.put(key, getInteger(summaryObj2, key));
    }

    // construct the new instance
    return new SzRelationship(entityId1,
                              entityId2,
                              matchLevel,
                              matchType,
                              matchKey,
                              principle,
                              summary1,
                              summary2,
                              false);
  }

  /**
   * Parses the specified JSON text.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The newly constructed {@link SzRelationship}.
   */
  public static SzRelationship parse(String jsonText) {
    if (jsonText == null) return null;
    return parse(parseJsonObject(jsonText));
  }

  /**
   * Parses the specified JSON text and returns an {@link SzRelationship}
   * describing the relationship.
   *
   * @param hashText The entity hash text describing the entity.
   * @return The {@link SzRelationship} that was populated.
   *
   */
  public static SzRelationship parseHash(String hashText)
  {
    if (hashText == null) return null;
    String jsonText = ZipUtilities.unzipText64(hashText);
    return parse(jsonText);
  }

  /**
   * Test main method.
   * 
   * @param args The command-line arguments.
   */
  public static void main(String[] args) {
    try {
      SzResolvedEntity resolved = new SzResolvedEntity();
      resolved.setEntityId(1);
      resolved.setEntityName("Foo Smith");
      resolved.addRecord(new SzRecord("FOO", "FOO-1", null, null));
      SzRelatedEntity related = new SzRelatedEntity();
      related.setEntityId(2);
      related.addRecord(new SzRecord("BAR", "BAR-1", null, null));
      related.setEntityName("Bar Jones");
      related.setMatchKey("ADDRESS+PHONE_NUMBER");
      related.setPrinciple("MFF");
      related.setMatchLevel(3);
      related.setMatchType(SzMatchType.POSSIBLE_RELATION);
      resolved.addRelatedEntity(related);

      SzRelationship relationship1 = new SzRelationship(resolved, related);
      String hash = relationship1.toHash();
      SzRelationship relationship2 = SzRelationship.parseHash(hash);

      System.out.println();
      System.out.println(relationship1);
      System.out.println();
      System.out.println(relationship2);
      System.out.println();
      System.out.println(relationship1.equals(relationship2));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
