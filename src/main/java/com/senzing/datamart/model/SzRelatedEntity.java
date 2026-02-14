package com.senzing.datamart.model;

import javax.json.*;

import com.senzing.util.JsonUtilities;

import java.util.Objects;

import static com.senzing.util.JsonUtilities.*;
import static com.senzing.util.LoggingUtilities.*;
/**
 * Extends {@link SzEntity} to describe a related entity.
 */
public class SzRelatedEntity extends SzEntity {
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
     * Default constructor.
     */
    public SzRelatedEntity() {
        this.matchType = null;
        this.matchKey = null;
        this.principle = null;
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
     * @param matchType The {@link SzMatchType} describing the relationship type for
     *                  this related entity.
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
        this.matchKey = normalize(matchKey);
    }

    /**
     * Gets the principle code identifying the Entity Resolution Rule that created
     * this relationship.
     *
     * @return The principle code identifying the Entity Resolution Rule that
     *         created this relationship.
     */
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * Sets the principle code identifying the Entity Resolution Rule that created
     * this relationship.
     *
     * @param principle The principle code identifying the Entity Resolution Rule
     *                  that created this relationship.
     */
    public void setPrinciple(String principle) {
        this.principle = normalize(principle);
    }

    /**
     * Populates the specified {@link JsonObjectBuilder} with the properties of this
     * instance.
     *
     * @param builder The {@link JsonObjectBuilder} to populate.
     */
    public void buildJson(JsonObjectBuilder builder) {
        super.buildJson(builder);
        builder.add("matchType", this.getMatchType().getCode());
        builder.add("matchKey", this.getMatchKey());
        builder.add("principle", this.getPrinciple());
    }

    /**
     * Parses the specified JSON and populates the specified
     * {@link SzRelatedEntity}.
     *
     * @param entity     The {@link SzRelatedEntity} to populate, or
     *                   <code>null</code> if a new {@link SzRelatedEntity} should
     *                   be created.
     * @param jsonObject The {@link JsonObject} describing the entity.
     * @return The {@link SzRelatedEntity} that was populated.
     */
    public static SzRelatedEntity parse(SzRelatedEntity entity, JsonObject jsonObject) {
        // get the entity to populate
        if (entity == null) {
            entity = new SzRelatedEntity();
        }
        SzEntity.parse(entity, jsonObject);

        // get and set the relationship type
        SzMatchType matchType = null;
        String matchTypeText = getString(jsonObject, "matchType");
        if (matchTypeText == null) {
            matchType = SzMatchType.detect(jsonObject);
        } else {
            matchType = SzMatchType.lookup(matchTypeText);
            if (matchType == null) {
                matchType = SzMatchType.valueOf(matchTypeText);
            }
        }
        entity.setMatchType(matchType);

        // get and set the match key
        String matchKey = getString(jsonObject, "matchKey");
        if (matchKey == null) {
            matchKey = getString(jsonObject, "MATCH_KEY");
        }
        if (matchKey == null || matchKey.trim().length() == 0) {
            logError("","---------------------",
                    "Encountered empty match key (" + matchKey + ") for relationship: ", 
                     JsonUtilities.toJsonText(jsonObject, true), 
                     "","---------------------", "");
        }
        entity.setMatchKey(matchKey);

        // get and set the principle
        String principle = getString(jsonObject, "principle");
        if (principle == null) {
            principle = getString(jsonObject, "ERRULE_CODE");
        }
        entity.setPrinciple(principle);

        // return the entity
        return entity;
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
        if (!super.equals(o)) {
            return false;
        }
        SzRelatedEntity that = (SzRelatedEntity) o;
        return this.getMatchType() == that.getMatchType()
                && Objects.equals(this.getMatchKey(), that.getMatchKey())
                && Objects.equals(this.getPrinciple(), that.getPrinciple());
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)}
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.getMatchType(), this.getMatchKey(),
                this.getPrinciple());
    }

    /**
     * Normalizes the specified text value so that if <code>null</code>
     * or empty string or purely whitespace, then <code>null<code> is
     * returned, otherwise the value is returned with leading and trailing
     * whitespace removed.
     * 
     * @param value The value to normalize.
     * 
     * @return The normalized value.
     */
    private static String normalize(String value) {
        if (value != null) {
            value = value.trim();
            if (value.length() == 0) {
                value = null;
            }
        }
        return value;
    }
}
