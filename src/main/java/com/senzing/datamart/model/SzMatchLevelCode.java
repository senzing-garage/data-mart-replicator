package com.senzing.datamart.model;

/**
 * Enumerates the various values for the <code>MATCH_LEVEL_CODE</code>
 * property in the Senzing JSON responses.
 */
public enum SzMatchLevelCode {
    /**
     * Indicates that two records resolved to the same entity.
     */
    RESOLVED,

    /**
     * Constitutes a relationship (or search match) that is based on one
     * or more strongly identifying matching attributes of entities, such
     * that the entities may represent the same entity.
     */
    POSSIBLY_SAME,

    /**
     * Constitutes a relationship (or search match) that is based on one
     * or more matching attributes of entities, but not indicating that
     * the entities may be the same.
     */
    POSSIBLY_RELATED,

    /**
     * Constitutes a relationship (or search match) that is based on only name.
     */
    NAME_ONLY,

    /**
     * Constitutes a disclosed relationship.
     */
    DISCLOSED;
}
