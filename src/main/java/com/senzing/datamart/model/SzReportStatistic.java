package com.senzing.datamart.model;

/**
 * Enumerates the report statistics for various reports (though some reports
 * have statistics that cannot be enumerated here).
 */
public enum SzReportStatistic {
  /**
   *
   */
  ENTITY_COUNT,

  /**
   *
   */
  UNMATCHED_COUNT,

  /**
   *
   */
  MATCHED_COUNT,

  /**
   *
   */
  AMBIGUOUS_MATCH_COUNT,

  /**
   *
   */
  POSSIBLE_MATCH_COUNT,

  /**
   *
   */
  DISCLOSED_RELATION_COUNT,

  /**
   *
   */
  POSSIBLE_RELATION_COUNT;
}
