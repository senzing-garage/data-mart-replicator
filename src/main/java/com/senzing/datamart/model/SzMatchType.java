package com.senzing.datamart.model;

import com.senzing.util.JsonUtilities;

import javax.json.JsonObject;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The various relationship match types for the data mart.
 */
public enum SzMatchType {
    /**
     * The relationship describes an ambiguous match.
     */
    AMBIGUOUS_MATCH("AM"),

    /**
     * The relationship describes a possible match.
     */
    POSSIBLE_MATCH("PM"),

    /**
     * The relationship describes a disclosed relationship.
     */
    DISCLOSED_RELATION("DR"),

    /**
     * The relationship describes a discovered possible relationship.
     */
    POSSIBLE_RELATION("PR");

    /**
     * Constructs with the short code identifying match type.
     * 
     * @param code The code with which to construct.
     */
    SzMatchType(String code) {
        this.code = code;
    }

    /**
     * The short code for this instance.
     */
    private String code;

    /**
     * Gets the code from this instance.
     * 
     * @return The code from this instance.
     */
    public String getCode() {
        return this.code;
    }

    /**
     * Gets the {@link SzMatchType} for the specified two-letter code. This returns
     * <code>null</code> if the specified code is not recognized.
     *
     * @param code The code for which the {@link SzMatchType} is being requested.
     * @return The {@link SzMatchType} for the specified code, or <code>null</code>
     *         if the specified code is not recognized.
     */
    public static SzMatchType lookup(String code) {
        return CODE_MAP.get(code.toUpperCase());
    }

    /**
     * The {@link Map} of two-letter codes to {@link SzMatchType} instances.
     */
    private static final Map<String, SzMatchType> CODE_MAP;

    /**
     * Initializes the code lookup map.
     */
    static {
        SzMatchType[] matchTypes = SzMatchType.values();
        Map<String, SzMatchType> map = new LinkedHashMap<>();
        for (SzMatchType matchType : matchTypes) {
            map.put(matchType.getCode().toUpperCase(), matchType);
        }
        CODE_MAP = Collections.unmodifiableMap(map);
    }

    /**
     * Detect the relationship type from the specified {@link JsonObject}.
     *
     * @param jsonObject The {@link JsonObject} to extract the relationship type
     *                   from.
     * @return The detected {@link SzMatchType}.
     */
    public static SzMatchType detect(JsonObject jsonObject) {
        if (jsonObject.containsKey("IS_AMBIGUOUS")) {
            if (jsonObject.getInt("IS_AMBIGUOUS") != 0) {
                return AMBIGUOUS_MATCH;
            }
        }

        if (jsonObject.containsKey("IS_DISCLOSED")) {
            if (jsonObject.getInt("IS_DISCLOSED") != 0) {
                return DISCLOSED_RELATION;
            }
        }
        int matchLevel = JsonUtilities.getInteger(jsonObject, "MATCH_LEVEL");

        // check the match level
        if (matchLevel == 2)
            return POSSIBLE_MATCH;

        // assume its a possible relation
        return POSSIBLE_RELATION;
    }

}
