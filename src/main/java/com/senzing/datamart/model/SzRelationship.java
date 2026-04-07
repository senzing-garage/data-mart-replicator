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
     * The entity ID, which is always the greatest of the two entity ID's.
     */
    private long entityId;

    /**
     * The related entity ID, which is always the greatest of the two
     * entity ID's.
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
     * The {@link Map} of {@link String} data source keys to
     * {@link Integer} values indicating the number of records having
     * that data source for the entity with the lower entity ID.
     */
    private Map<String, Integer> sourceSummary;

    /**
     * The {@link Map} of {@link String} data source keys to
     * {@link Integer} values indicating the number of records
     * having that data source for the entity with the greater entity ID.
     */
    private Map<String, Integer> relatedSourceSummary;

    /**
     * Constructs with the specified {@link SzResolvedEntity} and
     * {@link SzRelatedEntity} to create the relationship.
     *
     * @param resolvedEntity The {@link SzResolvedEntity} in the relationship.
     * @param relatedEntity  The {@link SzRelatedEntity} in the relationship.
     * @throws NullPointerException If either parameter is <code>null</code>.
     * @throws IllegalArgumentException If the specified {@link SzRelatedEntity}
     *                                  is not related to the specified
     *                                  {@link SzResolvedEntity}.
     */
    public SzRelationship(SzResolvedEntity  resolvedEntity, 
                          SzRelatedEntity   relatedEntity) 
    {
        Objects.requireNonNull(
            resolvedEntity, "The resolved entity cannot be null.");
        Objects.requireNonNull(
            relatedEntity, "The related entity cannot be null.");

        long relatedId = relatedEntity.getEntityId();
        if (!resolvedEntity.getRelatedEntities().containsKey(relatedId)) {
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
        boolean flip
            = (resolvedEntity.getEntityId() > relatedEntity.getEntityId());
        long resolvedEntityId = resolvedEntity.getEntityId();
        long relatedEntityId = relatedEntity.getEntityId();

        this.entityId = (flip) ? relatedEntityId : resolvedEntityId;
        this.relatedId = (flip) ? resolvedEntityId : relatedEntityId;

        this.matchKey = (flip) ? this.reverseMatchKey : this.matchKey;
        this.reverseMatchKey = (flip) ? relatedEntity.getMatchKey() 
                                      : this.reverseMatchKey;

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
     * @param matchType The non-null {@link SzMatchType} for the relationship.
     * @param matchKey The non-null match key for the relationship.
     * @param reverseMatchKey The non-null reverse match key for the 
     *                        relationship.
     * @param principle The principle (ER Rule Code) for the relationship.
     * @param sourceSummary1 The {@link Map} of {@link String} data source
     *                       code keys to {@link Integer} record counts for
     *                       the first entity ID.
     * @param sourceSummary2 The {@link Map} of {@link String} data source
     *                       code keys to {@link Integer} record counts for
     *                       the second entity ID.
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
     * @param entityId1 The first entity ID for the relationship.
     * @param entityId2 The second entity ID for the relationship.
     * @param matchType The non-null {@link SzMatchType} for the relationship.
     * @param matchKey The non-null match key for the relationship.
     * @param reverseMatchKey The non-null reverse match key for the 
     *                        relationship.
     * @param principle The principle (ER Rule Code) for the relationship.
     * @param sourceSummary1 The {@link Map} of {@link String} data source
     *                       code keys to {@link Integer} record counts for
     *                       the first entity ID.
     * @param sourceSummary2 The {@link Map} of {@link String} data source
     *                       code keys to {@link Integer} record counts for
     *                       the second entity ID.
     * @param copyMaps <code>true</code> if the specified maps should be
     *                 copied, or <code>false</code> if the referenced maps
     *                 should be used directly.
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
        Objects.requireNonNull(
            matchKey, "The match key cannot be null.");
        Objects.requireNonNull(
            reverseMatchKey, "The reverse match key cannot be null.");
        Objects.requireNonNull(
            principle, "The principle cannot be null.");
        Objects.requireNonNull(
            matchType, "The match type cannot be null.");
        Objects.requireNonNull(
            sourceSummary1, "The first source summary cannot be null");
        Objects.requireNonNull(
            sourceSummary2, "The second source summary cannot be null");

        boolean flip = (entityId2 < entityId1);

        this.entityId = (flip) ? entityId2 : entityId1;
        this.relatedId = (flip) ? entityId1 : entityId2;
        this.matchType = matchType;
        this.matchKey = matchKey;
        this.reverseMatchKey = reverseMatchKey;
        this.principle = principle;

        Map<String, Integer> summary1
            = (flip) ? sourceSummary2 : sourceSummary1;

        Map<String, Integer> summary2
            = (flip) ? sourceSummary1 : sourceSummary2;

        this.sourceSummary
            = (copyMaps) ? new LinkedHashMap<>(summary1) : summary1;

        this.relatedSourceSummary
            = (copyMaps) ? new LinkedHashMap<>(summary2) : summary2;
    }

    /**
     * Reverses a match key by swapping the min/max roles in any
     * role-bearing components (those with parenthesized {@code min:max}
     * values). Non-role-bearing components (e.g., {@code +NAME},
     * {@code -DOB}) pass through unchanged.
     *
     * <p>This handles match keys that combine discovered and disclosed
     * relationship components, e.g.:</p>
     * <ul>
     *   <li>{@code +NAME+ADDRESS+REL_POINTER(:SUBSIDIARY_OF)} &rarr;
     *       {@code +NAME+ADDRESS+REL_POINTER(SUBSIDIARY_OF:)}</li>
     *   <li>{@code +REL_POINTER(GLOBAL\:PARENT:SUBSIDIARY)} &rarr;
     *       {@code +REL_POINTER(SUBSIDIARY:GLOBAL\:PARENT)}</li>
     * </ul>
     *
     * <p>Special characters in role values are escaped with backslash
     * by the Senzing engine (as of 4.3.0). The structural colon
     * separating min/max roles is never escaped, making it the only
     * unescaped colon inside the parentheses.</p>
     *
     * @param matchKey The original match key.
     *
     * @return The reversed match key, or the original if no role-bearing
     *         components are found or the match key is {@code null}.
     */
    public static String getReverseMatchKey(String matchKey) {
        if (matchKey == null) {
            return null;
        }

        String key = matchKey.trim();
        if (key.isEmpty()) {
            return matchKey;
        }

        char[] text = key.toCharArray();
        int len = text.length;

        // Split into components on unescaped '+' and '-' boundaries
        // and reverse any role-bearing components
        StringBuilder result = new StringBuilder(len);
        int componentStart = 0;

        for (int i = 0; i <= len; i++) {
            boolean atBoundary;
            if (i == len) {
                atBoundary = true;
            } else if (i == 0) {
                atBoundary = false;
            } else if (text[i] == '+' || text[i] == '-') {
                // check if this '+' or '-' is escaped
                atBoundary = !isEscaped(text, i);
            } else {
                atBoundary = false;
            }

            if (atBoundary) {
                // process the component from componentStart to i
                String component = key.substring(componentStart, i);
                result.append(reverseRoleBearingComponent(component));
                componentStart = i;
            }
        }

        return result.toString();
    }

    /**
     * Checks if the character at the specified index is escaped by
     * counting preceding backslashes. An odd number of preceding
     * backslashes means the character is escaped.
     *
     * @param text  The character array.
     * @param index The index to check.
     * @return {@code true} if the character at {@code index} is escaped.
     */
    private static boolean isEscaped(char[] text, int index) {
        int backslashCount = 0;
        for (int j = index - 1; j >= 0 && text[j] == '\\'; j--) {
            backslashCount++;
        }
        return (backslashCount % 2) != 0;
    }

    /**
     * Finds the index of the next unescaped occurrence of the target
     * character in the specified range of the character array.
     *
     * @param text   The character array to search.
     * @param target The character to find.
     * @param start  The start index (inclusive).
     * @param end    The end index (exclusive).
     * @return The index of the next unescaped occurrence, or {@code -1}.
     */
    private static int findNextUnescaped(char[] text, char target,
                                         int start, int end) {
        for (int i = start; i < end; i++) {
            if (text[i] == target && !isEscaped(text, i)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Reverses a single match key component if it is role-bearing
     * (i.e., contains a parenthesized {@code min:max} section with
     * exactly one unescaped colon). Non-role-bearing components are
     * returned unchanged.
     *
     * @param component The match key component (e.g.,
     *                  {@code "+REL_POINTER(A:B)"} or {@code "+NAME"}).
     * @return The reversed component, or the original if not role-bearing.
     */
    private static String reverseRoleBearingComponent(String component) {
        if (component == null || component.isEmpty()) {
            return component;
        }

        char[] text = component.toCharArray();
        int len = text.length;

        // Find unescaped opening parenthesis
        int openParen = findNextUnescaped(text, '(', 0, len);
        if (openParen < 0) {
            return component; // no parenthesized section
        }

        // The '(' must immediately follow the identifier with no whitespace.
        // This distinguishes role-bearing components like
        // +REL_POINTER(min:max) from the " (Ambiguous)" suffix the engine
        // appends to ambiguous match keys.
        if (openParen == 0 || Character.isWhitespace(text[openParen - 1])) {
            return component;
        }

        // Find the first unescaped closing parenthesis after the opening one.
        // We search forward (not from the end) so that a trailing suffix like
        // " (Ambiguous)" does not capture the wrong closing paren.
        int closeParen = findNextUnescaped(
            text, ')', openParen + 1, len);
        if (closeParen < 0) {
            logWarning("Missing closing parenthesis in match key component: "
                       + component);
            return component;
        }

        // Find the single unescaped colon between the parentheses
        int roleStart = openParen + 1;
        int roleEnd = closeParen;

        // Find exactly one unescaped colon inside the parentheses
        int colonIndex = -1;
        int colonCount = 0;
        for (int i = roleStart; i < roleEnd; i++) {
            if (text[i] == ':' && !isEscaped(text, i)) {
                if (colonCount == 0) {
                    colonIndex = i;
                }
                colonCount++;
                if (colonCount > 1) {
                    break; // no need to keep counting
                }
            }
        }

        if (colonCount == 0) {
            logWarning("No unescaped colon found in role-bearing component: "
                       + component);
            return component;
        }

        if (colonCount > 1) {
            logWarning("Multiple unescaped colons in role-bearing component: "
                       + component);
            return component;
        }

        // Extract the min and max role parts
        String prefix = component.substring(0, openParen + 1);
        String minPart = component.substring(roleStart, colonIndex);
        String maxPart = component.substring(colonIndex + 1, closeParen);
        String suffix = component.substring(closeParen);

        // Swap min and max
        return prefix + maxPart + ":" + minPart + suffix;
    }

    /**
     * Gets the lesser of the two entity ID's so the first entity ID is always
     * the lesser of the two for normalizing the relationship properties.
     *
     * @return The lesser of the two entity ID's so the first entity ID is
     *         always the lesser of the two for normalizing the relationship
     *         properties.
     */
    public long getEntityId() {
        return this.entityId;
    }

    /**
     * Gets the greater of the two entity ID's so the related ID is always the
     * greater of the two for normalizing the relationship properties.
     *
     * @return The greater of the two entity ID's so the related ID is always
     *         the greater of the two for normalizing the relationship
     *         properties.
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
     * Gets the principle code identifying the Entity Resolution Rule that
     * created this relationship.
     *
     * @return The principle code identifying the Entity Resolution Rule
     *         that created this relationship.
     */
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} record count values describing the number of records
     * per data source for the entity in the relationship having the lesser
     * entity ID.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} record count values describing the number of
     *         records per data source for the entity in the relationship
     *         having the lesser entity ID.
     */
    public Map<String, Integer> getSourceSummary() {
        return Collections.unmodifiableMap(this.sourceSummary);
    }

    /**
     * Gets the {@link Map} of {@link String} data source code keys to
     * {@link Integer} record count values describing the number of records
     * per data source for the entity in the relationship having the greater
     * entity ID.
     *
     * @return The {@link Map} of {@link String} data source code keys to
     *         {@link Integer} record count values describing the number of
     *         records per data source for the entity in the relationship
     *         having the greater entity ID.
     */
    public Map<String, Integer> getRelatedSourceSummary() {
        return Collections.unmodifiableMap(this.relatedSourceSummary);
    }

    /**
     * Overridden to return <code>true</code> if and only if the specified
     * parameter is an instance of the same class with equivalent properties.
     * 
     * @param object The object to compare with.
     * @return <code>true</code> if the specified parameter is an instance
     *         of the same class with equivalent properties, otherwise
     *         <code>false</code>.
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
        return getEntityId() == rel.getEntityId() 
                && this.getRelatedEntityId() == rel.getRelatedEntityId()
                && this.getMatchType() == rel.getMatchType()
                && Objects.equals(this.getMatchKey(), rel.getMatchKey())
                && Objects.equals(this.getReverseMatchKey(), 
                                  rel.getReverseMatchKey())
                && Objects.equals(this.getPrinciple(), rel.getPrinciple())
                && this.getSourceSummary().equals(rel.getSourceSummary())
                && this.getRelatedSourceSummary().equals(
                    rel.getRelatedSourceSummary());
    }

    /**
     * Overridden to return a hash code consistent with the
     * {@link #equals(Object)} implementation.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getEntityId(), 
                            this.getRelatedEntityId(),
                            this.getMatchType(),
                            this.getMatchKey(),
                            this.getReverseMatchKey(),
                            this.getPrinciple(),
                            this.getSourceSummary(),
                            this.getRelatedSourceSummary());
    }

    /**
     * Adds the JSON properties for this instance to the specified
     * {@link JsonObjectBuilder}.
     *
     * @param builder The {@link JsonObjectBuilder} to which the properties
     *                will be added.
     */
    public void buildJson(JsonObjectBuilder builder) {
        builder.add("entityId", this.getEntityId());
        builder.add("relatedId", this.getRelatedEntityId());
        if (this.getMatchType() != null) {
            builder.add("matchType", this.getMatchType().toString());
        }
        if (this.getMatchKey() != null) {
            builder.add("matchKey", this.getMatchKey());
        }
        if (this.getReverseMatchKey() != null) {
            builder.add("reverseMatchKey", this.getReverseMatchKey());
        }
        if (this.getPrinciple() != null) {
            builder.add("principle", this.getPrinciple());
        }
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
     * Converts this instance to JSON text.
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
