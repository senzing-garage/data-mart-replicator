package com.senzing.datamart.model;

/**
 * Enumerates the report statistics for various reports (though some reports
 * have statistics that cannot be enumerated here).
 */
public enum SzReportStatistic {
  /**
   * Describes the number of entities pertaining to the statistic.
   */
  ENTITY_COUNT,

  /**
   * Describes the number of entities having only a single record.
   */
  UNMATCHED_COUNT,

  /**
   * Describes the number of entities for which two records matched for
   * two specific data sources (possibly the same data source).
   */
  MATCHED_COUNT,

  /**
   * Describes the number of entities for which there is an ambiguous
   * match.
   */
  AMBIGUOUS_MATCH_COUNT,

  /**
   * Describes the number of related entity pairs that are possible 
   * matches for two specific data sources (possibly the same data source).
   */
  POSSIBLE_MATCH_COUNT,

  /**
   * Describes the number of related entity pairs that are related by 
   * disclosed relationships for two specific data sources (possibly
   * the same data source).
   */
  DISCLOSED_RELATION_COUNT,

  /**
   * Describes the number of related entity pairs that are related by 
   * possible relationships for two specific data sources (possibly
   * the same data source).
   */
  POSSIBLE_RELATION_COUNT;
}
