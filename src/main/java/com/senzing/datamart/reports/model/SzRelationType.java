package com.senzing.datamart.reports.model;

/**
 * The various relationship match types for the data mart.
 */
public enum SzRelationType {
    /**
     * The relationship describes an ambiguous match.
     */
    AMBIGUOUS_MATCH,

    /**
     * The relationship describes a possible match.
     */
    POSSIBLE_MATCH,
    
    /**
     * The relationship describes a disclosed relationship.
     */
    POSSIBLE_RELATION,

    /**
     * The relationship describes a discovered possible relationship.
     */
    DISCLOSED_RELATION;
}
