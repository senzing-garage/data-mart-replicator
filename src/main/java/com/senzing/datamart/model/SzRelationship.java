package com.senzing.datamart.model;

import com.senzing.util.JsonUtilities;
import com.senzing.util.ZipUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

import static com.senzing.datamart.model.SzMatchType.DISCLOSED_RELATION;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Describes a relationship between two entities as it is stored in the data
 * mart.
 */
public class SzRelationship {
    /**
     * The prefix for <code>"REL_POINTER"</code> match keys.
     */
    private static final String REL_POINTER_PREFIX = "+REL_POINTER(";

    /**
     * The entity ID, which is always the greatest of the two entity ID's.
     */
    private long entityId;

    /**
     * The related entity ID, which is always the greatest of the two entity ID's.
     */
    private long relatedId;

    /**
     * The {@link SzMatchType} for this related entity.
     */
    private SzMatchType matchType;

    /**
     * The match key for this related entity.
     */
    private String matchKey;

    /**
     * The match key from the greater entity to the lesser entity to
     * accommodate directional match keys.
     */
    private String reverseMatchKey;

    /**
     * The principle used for relating the entities.
     */
    private String principle;

    /**
     * The {@link Map} of {@link String} data source keys to {@link Integer} values
     * indicating the number of records having that data source for the entity with
     * the lower entity ID.
     */
    private Map<String, Integer> sourceSummary;

    /**
     * The {@link Map} of {@link String} data source keys to {@link Integer} values
     * indicating the number of records having that data source for the entity with
     * the greater entity ID.
     */
    private Map<String, Integer> relatedSourceSummary;

    /**
     * Constructs with the specified {@link SzResolvedEntity} and
     * {@link SzRelatedEntity} to create the relationship.
     *
     * @param resolvedEntity The {@link SzResolvedEntity} in the relationship.
     * @param relatedEntity  The {@link SzRelatedEntity} in the relationship.
     * @throws NullPointerException     If either parameter is <code>null</code>.
     * @throws IllegalArgumentException If the specified {@link SzRelatedEntity} is
     *                                  not related to the specified
     *                                  {@link SzResolvedEntity}.
     */
    public SzRelationship(SzResolvedEntity  resolvedEntity, 
                          SzRelatedEntity   relatedEntity) 
    {
        Objects.requireNonNull(resolvedEntity, "The resolved entity cannot be null.");
        Objects.requireNonNull(relatedEntity, "The related entity cannot be null.");
        if (!resolvedEntity.getRelatedEntities().containsKey(relatedEntity.getEntityId())) {
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

        this.matchType = relatedEntity.getMatchType();
        this.matchKey = relatedEntity.getMatchKey();
        this.reverseMatchKey = this.matchKey;
        this.principle = relatedEntity.getPrinciple();
        if (this.matchType.equals(DISCLOSED_RELATION)) {
            this.reverseMatchKey = getReverseMatchKey(this.matchKey);
        }
        boolean flip = (resolvedEntity.getEntityId() > relatedEntity.getEntityId());
        long resolvedEntityId = resolvedEntity.getEntityId();
        long relatedEntityId = relatedEntity.getEntityId();

        this.entityId = (flip) ? relatedEntityId : resolvedEntityId;
        this.relatedId = (flip) ? resolvedEntityId : relatedEntityId;

        this.matchKey = (flip) ? this.reverseMatchKey : this.matchKey;
        this.reverseMatchKey = (flip) ? relatedEntity.getMatchKey() : this.reverseMatchKey;

        this.sourceSummary = (flip) ? relatedEntity.getSourceSummary() : resolvedEntity.getSourceSummary();
        this.relatedSourceSummary = (flip) ? resolvedEntity.getSourceSummary() : relatedEntity.getSourceSummary();
    }

    /**
     * Constructs with the specified properties.
     *
     * @param entityId1       The first entity ID for the relationship.
     * @param entityId2       The second entity ID for the relationship.
     * @param matchType       The non-null {@link SzMatchType} for the relationship.
     * @param matchKey        The non-null match key for the relationship.
     * @param reverseMatchKey The non-null reverse match key for the relationship.
     * @param principle       The principle (ER Rule Code) for the relationship.
     * @param sourceSummary1  The {@link Map} of {@link String} data source code
     *                        keys to {@link Integer} record counts for the first
     *                        entity ID.
     * @param sourceSummary2  The {@link Map} of {@link String} data source code
     *                        keys to {@link Integer} record counts for the second
     *                        entity ID.
     */
    SzRelationship(long                 entityId1, 
                   long                 entityId2, 
                   SzMatchType          matchType,
                   String               matchKey, 
                   String               reverseMatchKey,
                   String               principle,
                   Map<String, Integer> sourceSummary1,
                   Map<String, Integer> sourceSummary2)
            throws NullPointerException, IllegalArgumentException 
    {
        this(entityId1, entityId2, matchType, matchKey, reverseMatchKey,
             principle, sourceSummary1, sourceSummary2, true);
    }

    /**
     * Private constructor to avoid copying the specified maps.
     *
     * @param entityId1       The first entity ID for the relationship.
     * @param entityId2       The second entity ID for the relationship.
     * @param matchType       The non-null {@link SzMatchType} for the relationship.
     * @param matchKey        The non-null match key for the relationship.
     * @param reverseMatchKey The non-null reverse match key for the relationship.
     * @param principle       The principle (ER Rule Code) for the relationship.
     * @param sourceSummary1  The {@link Map} of {@link String} data source code
     *                        keys to {@link Integer} record counts for the first
     *                        entity ID.
     * @param sourceSummary2  The {@link Map} of {@link String} data source code
     *                        keys to {@link Integer} record counts for the second
     *                        entity ID.
     * @param copyMaps        <code>true</code> if the specified maps should be
     *                        copied, or <code>false</code> if the referenced maps
     *                        should be used directly.
     */
    private SzRelationship(long                 entityId1, 
                           long                 entityId2, 
                           SzMatchType          matchType,
                           String               matchKey,
                           String               reverseMatchKey,
                           String               principle, 
                           Map<String, Integer> sourceSummary1,
                           Map<String, Integer> sourceSummary2,
                           boolean              copyMaps)
            throws NullPointerException, IllegalArgumentException {
        Objects.requireNonNull(matchKey, "The match key cannot be null.");
        Objects.requireNonNull(reverseMatchKey, "The reverse match key cannot be null.");
        Objects.requireNonNull(principle, "The principle cannot be null.");
        Objects.requireNonNull(matchType, "The match type cannot be null.");
        Objects.requireNonNull(sourceSummary1, "The first source summary cannot be null");
        Objects.requireNonNull(sourceSummary2, "The second source summary cannot be null");

        boolean flip = (entityId2 < entityId1);

        this.entityId = (flip) ? entityId2 : entityId1;
        this.relatedId = (flip) ? entityId1 : entityId2;
        this.matchType = matchType;
        this.matchKey = matchKey;
        this.reverseMatchKey = reverseMatchKey;
        this.principle = principle;

        Map<String, Integer> summary1 = (flip) ? sourceSummary2 : sourceSummary1;
        Map<String, Integer> summary2 = (flip) ? sourceSummary1 : sourceSummary2;

        this.sourceSummary = (copyMaps) ? new LinkedHashMap<>(summary1) : summary1;

        this.relatedSourceSummary = (copyMaps) ? new LinkedHashMap<>(summary2) : summary2;
    }

    /**
     * Reverses the specified <code>"REL_POINTER"</code> match key if it is
     * formatted as expected, otherwise returns the specified match key and
     * logs warnings.  This handles changing a match key that looks like
     * <code>"+REL_POINTER(A:B)"</code> to <code>"+REL_POINTER(B:A)"</code>,
     * but typically it would change <code>"+REL_POINTER(ABC:)"</code> to
     * <code>"+REL_POINTER(:ABC)"</code> since usually only one side of the
     * colon is populated in a <code>"REL_POINTER"</code> match key.
     * 
     * @param matchKey The original match key.
     * 
     * @return The reversed match key if the specified match key is in the
     *         expected format, otherwise the specified match key.
     */
    public static final String getReverseMatchKey(String matchKey) {
        // check if null
        if (matchKey == null) {
            return null;
        }

        // trim the match key
        String key = matchKey.trim();

        if (!key.startsWith(REL_POINTER_PREFIX)) {
            return matchKey;
        }

        // check that our match key is longer than the prefix by at
        // least 3 characters (colon and parentheses and one other)
        int prefixLength = REL_POINTER_PREFIX.length();
        if (key.length() < (prefixLength + 3)) {
            logWarning("Badly formatted REL_POINTER match key: " + matchKey);
            return matchKey;
        }

        // get the characters
        char[] text = key.toCharArray();

        // find the starting index
        int startingIndex = prefixLength;
        for (; startingIndex < text.length; startingIndex++) {
            if (!Character.isWhitespace(text[startingIndex])) {
                break;
            }
        }

        // check if all whitespace
        if (startingIndex >= text.length) {
            logWarning("Unexpected format for REL_POINTER match key: " + matchKey);
            return matchKey;
        }

        // find the ending index
        int endingIndex = key.length() - 1;
        for (; endingIndex > 0; endingIndex--) {
            // skip the whitespace
            if (Character.isWhitespace(text[endingIndex])) {
                continue;
            }

            // check if a closing parentheses
            if (text[endingIndex] != ')') {
                endingIndex = -1;
            }

            // we got a non-whitespace character, so we are done
            break;
        }

        // check if no ending index
        if (endingIndex == startingIndex) {
            logWarning("Failed to find contents in REL_POINTER match key: " + matchKey);
            return matchKey;
        }
        if (endingIndex < startingIndex) {
            logWarning("Failed to find closing parentheses in REL_POINTER match key: " + matchKey);
            return matchKey;
        }

        // track all the colon indexes
        List<Integer> colonIndexes = new ArrayList<>();

        // find all the colon indexes and closing parentheses
        boolean escaping = false;
        for (int index = startingIndex; index < endingIndex; index++) {
            if (!escaping) {
                switch (text[index]) {
                    case '\\':
                        escaping = true;
                        break;
                    case ':':
                        colonIndexes.add(index);
                        break;
                    default:
                        // do nothing
                }
            } else {
                escaping = false;
            }
        }

        // check if we have a single unescaped colon
        if (colonIndexes.size() == 0) {
            logWarning("Failed to find unescaped colon in REL_POINTER match key: " + matchKey);
            return matchKey;            
        }
        
        // check for the common base case
        if (colonIndexes.size() == 1) {
            // get the parts
            int colonIndex = colonIndexes.get(0);
            String part1 = key.substring(prefixLength, colonIndex);
            String part2 = key.substring(colonIndex + 1, endingIndex);
            return REL_POINTER_PREFIX + part2 + ":" + part1 + ")";
        }

        // handle multiple multiple colons
        int firstColon = colonIndexes.get(0);
        int lastColon = colonIndexes.get(colonIndexes.size() - 1);

        // check if we have a leading, but not trailing colon
        if (firstColon == prefixLength && lastColon != endingIndex - 1) {
            String part1 = key.substring(prefixLength + 1, lastColon);
            String part2 = key.substring(lastColon + 1, endingIndex);
            return REL_POINTER_PREFIX + part2 + ":" + part1 + ")";
 
        }
    
        // check if we have a trailing, but not a leading colon
        if (lastColon == endingIndex - 1 && firstColon != prefixLength) {
            String part1 = key.substring(prefixLength + 1, firstColon);
            String part2 = key.substring(firstColon + 1, endingIndex - 1);
            return REL_POINTER_PREFIX + part2 + ":" + part1 + ")";
        }
        logWarning("Found more than one colon in REL_POINTER match key: " 
                   + matchKey);
        return matchKey;
    }

    /**
     * Finds the index of the first non-whitespace character in 
     * the specified text starting at the specified index (inclusive)
     * and searching towards the end of the text.
     * 
     * @param text The text to search.
     * @param startIndex The starting index to begin the search.
     * @return The index of the first non-whitespace character,
     *         or the length of the specified text if not found.
     */
    protected static int eatWhiteSpace(char[] text, int startIndex) {
        for (int index = startIndex; index < text.length; index++) {
            if (!Character.isWhitespace(text[index])) {
                return index;
            }
        }
        return text.length;
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
     * Gets the reverse match key from the greater entity ID to the lesser
     * entity ID.  This will usually be the same as the match key except in
     * the case of {@linkplain SzMatchType#DISCLOSED_RELATION disclosed 
     * relationship} with a <code>"REL_POINTER"</code> match key which is
     * directional.
     * 
     * @return The match key from the greater entity ID to the lesser 
     *         entity ID to accommodate directional match keys.
     */
    public String getReverseMatchKey() {
        return this.reverseMatchKey;
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
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} record count values describing the number of records per data
     * source for the entity in the relationship having the lesser entity ID.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} record count values describing the number of records
     *         per data source for the entity in the relationship having the lesser
     *         entity ID.
     */
    public Map<String, Integer> getSourceSummary() {
        return Collections.unmodifiableMap(this.sourceSummary);
    }

    /**
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} record count values describing the number of records per data
     * source for the entity in the relationship having the greater entity ID.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} record count values describing the number of records
     *         per data source for the entity in the relationship having the greater
     *         entity ID.
     */
    public Map<String, Integer> getRelatedSourceSummary() {
        return Collections.unmodifiableMap(this.relatedSourceSummary);
    }

    /**
     * Overridden to return <code>true</code> if and only if the specified parameter
     * is an instance of the same class with equivalent properties.
     * 
     * @param object The object to compare with.
     * @return <code>true</code> if the specified parameter is an instance of the
     *         same class with equivalent properties, otherwise <code>false</code>.
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || this.getClass() != object.getClass()) {
            return false;
        }
        SzRelationship rel = (SzRelationship) object;
        return getEntityId() == rel.getEntityId() && this.getRelatedEntityId() == rel.getRelatedEntityId()
                && this.getMatchType() == rel.getMatchType()
                && Objects.equals(this.getMatchKey(), rel.getMatchKey())
                && Objects.equals(this.getReverseMatchKey(), rel.getReverseMatchKey())
                && Objects.equals(this.getPrinciple(), rel.getPrinciple())
                && this.getSourceSummary().equals(rel.getSourceSummary())
                && this.getRelatedSourceSummary().equals(rel.getRelatedSourceSummary());
    }

    /**
     * Overridden to return a hash code consistent with the {@link #equals(Object)}
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getEntityId(), this.getRelatedEntityId(), this.getMatchType(),
                this.getMatchKey(), this.getReverseMatchKey(), this.getPrinciple(),
                this.getSourceSummary(), this.getRelatedSourceSummary());
    }

    /**
     * Adds the JSON properties for this instance to the specified
     * {@link JsonObjectBuilder}.
     *
     * @param builder The {@link JsonObjectBuilder} to which the properties will be
     *                added.
     */
    public void buildJson(JsonObjectBuilder builder) {
        builder.add("entityId", this.getEntityId());
        builder.add("relatedId", this.getRelatedEntityId());
        builder.add("matchType", this.getMatchType().toString());
        builder.add("matchKey", this.getMatchKey());
        builder.add("reverseMatchKey", this.getReverseMatchKey());
        builder.add("principle", this.getPrinciple());
        SortedMap<String, Integer> sortedSummary = new TreeMap<>(this.getSourceSummary());
        addProperty(builder, "sourceSummary", sortedSummary);
        SortedMap<String, Integer> sortedRelatedSummary = new TreeMap<>(this.getRelatedSourceSummary());
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
        if (jsonObject == null) {
            return null;
        }

        Long entityId1 = getLong(jsonObject, "entityId");
        Long entityId2 = getLong(jsonObject, "relatedId");
        String typeString = getString(jsonObject, "matchType");
        String matchKey = getString(jsonObject, "matchKey");
        String revMatchKey = getString(jsonObject, "reverseMatchKey");
        String principle = getString(jsonObject, "principle");
        SzMatchType matchType = SzMatchType.valueOf(typeString);

        // get the first summary object
        JsonObject summaryObj1 = getJsonObject(jsonObject, "sourceSummary");
        Map<String, Integer> summary1 = new LinkedHashMap<>();
        for (String key : summaryObj1.keySet()) {
            summary1.put(key, getInteger(summaryObj1, key));
        }

        // get the second summary object
        JsonObject summaryObj2 = getJsonObject(jsonObject, "relatedSummary");
        Map<String, Integer> summary2 = new LinkedHashMap<>();
        for (String key : summaryObj2.keySet()) {
            summary2.put(key, getInteger(summaryObj2, key));
        }

        // construct the new instance
        return new SzRelationship(
            entityId1, entityId2, matchType, matchKey, revMatchKey, principle, 
            summary1, summary2, false);
    }

    /**
     * Parses the specified JSON text.
     *
     * @param jsonText The JSON text to parse.
     *
     * @return The newly constructed {@link SzRelationship}.
     */
    public static SzRelationship parse(String jsonText) {
        if (jsonText == null) {
            return null;
        }
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
    public static SzRelationship parseHash(String hashText) {
        if (hashText == null) {
            return null;
        }
        String jsonText = ZipUtilities.unzipText64(hashText);
        return parse(jsonText);
    }
}
