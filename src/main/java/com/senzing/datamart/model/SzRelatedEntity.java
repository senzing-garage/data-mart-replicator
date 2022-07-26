package com.senzing.datamart.model;

import javax.json.*;

import java.util.Objects;

import static com.senzing.util.JsonUtilities.*;

/**
 * Extends {@link SzEntity} to describe a related entity.
 */
public class SzRelatedEntity extends SzEntity {
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
   * Default constructor.
   */
  public SzRelatedEntity() {
    this.matchLevel = 0;
    this.matchType  = null;
    this.matchKey   = null;
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
   * Sets the match level for this related entity.
   *
   * @param matchLevel The match level for this related entity.
   */
  public void setMatchLevel(int matchLevel) {
    this.matchLevel = matchLevel;
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
   * Sets the relationship type for this related entity to the specified
   * {@link SzMatchType}.
   *
   * @param matchType The {@link SzMatchType} describing the relationship type
   *                  for this related entity.
   */
  public void setMatchType(SzMatchType matchType) {
    this.matchType = matchType;
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
   * Sets the match key for this related entity.
   *
   * @param matchKey The match key for this related entity.
   */
  public void setMatchKey(String matchKey) {
    this.matchKey = matchKey;
  }

  /**
   * Populates the specified {@link JsonObjectBuilder} with the properties
   * of this instance.
   *
   * @param builder The {@link JsonObjectBuilder} to populate.
   */
  public void buildJson(JsonObjectBuilder builder) {
    super.buildJson(builder);
    builder.add("matchLevel", this.getMatchLevel());
    builder.add("matchKey", this.getMatchKey());
    builder.add("matchType", this.getMatchType().getCode());
  }

  /**
   * Parses the specified JSON and populates the specified {@link
   * SzRelatedEntity}.
   *
   * @param entity The {@link SzRelatedEntity} to populate, or <code>null</code>
   *               if a new {@link SzRelatedEntity} should be created.
   * @param jsonObject The {@link JsonObject} describing the entity.
   * @return The {@link SzRelatedEntity} that was populated.
   */
  public static SzRelatedEntity parse(SzRelatedEntity entity,
                                      JsonObject      jsonObject)
  {
    // get the entity to populate
    if (entity == null) {
      entity = new SzRelatedEntity();
    }
    SzEntity.parse(entity, jsonObject);

    // get and set the match level
    Integer matchLevel = getInteger(jsonObject, "matchLevel");
    if (matchLevel == null) {
      matchLevel = getInteger(jsonObject, "MATCH_LEVEL");
    }
    entity.setMatchLevel(matchLevel);

    // get and set the relationship type
    SzMatchType matchType = null;
    String matchTypeText = getString(jsonObject, "matchType");
    if (matchTypeText == null) {
      matchType = SzMatchType.detect(jsonObject);
    } else {
      matchType = SzMatchType.valueOf(matchTypeText);
    }
    entity.setMatchType(matchType);

    // get and set the match key
    String matchKey = getString(jsonObject, "matchKey");
    if (matchKey == null) {
      matchKey = getString(jsonObject, "MATCH_KEY");
    }
    entity.setMatchKey(matchKey);

    // return the entity
    return entity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SzRelatedEntity that = (SzRelatedEntity) o;
    return this.getMatchLevel() == that.getMatchLevel()
        && this.getMatchType() == that.getMatchType()
        && Objects.equals(this.getMatchKey(), that.getMatchKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(),
                        this.getMatchLevel(),
                        this.getMatchType(),
                        this.getMatchKey());
  }
}
