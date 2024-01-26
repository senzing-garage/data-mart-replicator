package com.senzing.datamart.model;

/**
 * Enumerates the report statistics for various reports (though some reports
 * have statistics that cannot be enumerated here).
 */
public enum SzReportStatistic {
  /**
   * Provides a formatter to format {@link SzReportStatistic} instances 
   * with optional principles and match keys.
   */
  static class Formatter {
    /**
     * Constructs a new formatter with the specified {@link SzReportStats}
     */
    private Formatter(SzReportStatistic statistic) {
      this.statistic = statistic;
    }

    /**
     * The {@link SzReportStatistic} associated with the formatter.
     */
    private SzReportStatistic statistic;

    /**
     * The principle associated with the formatter.
     */
    private String principle;

    /**
     * The match key associated with the formatter.
     */
    private String matchKey;

    /**
     * Adds a principle to format with the statistic.
     * 
     * @param principle The principle to format with the statistic.
     * 
     * @return This {@link Formatter} instance.
     */
    public Formatter principle(String principle) {
      if (this.principle != null) {
        principle = principle.trim();
        if (principle.length() == 0) principle = null;
      }
      this.principle = principle;
      return this;
    }

    /**
     * Adds a match key to format with the statistic.
     * 
     * @param matchKey The match key to format with the statistic.
     * 
     * @return This {@link Formatter} instance.
     */
    public Formatter matchKey(String matchKey) {
      if (this.matchKey != null) {
        matchKey = matchKey.trim();
        if (matchKey.length() == 0) matchKey = null;
      }
      this.matchKey = matchKey;
      return this;
    }

    /**
     * Converts this instance to a {@link String} statistic.
     * 
     * @return The formatted {@link String} statistic for this instance.
     */
    public String format() {
      if (this.principle == null && this.matchKey == null) {
        return this.statistic.toString();
      }
      return this.statistic + ":" + this.principle + ":" + this.matchKey;
    }

    /**
     * Overridden to return the result from {@link #format()}.
     */
    public String toString() {
      return this.format();
    }
  }
  
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

  /**
   * Creates a {@link Formatter} that will format this {@link SzReportStatistic}
   * instance with the specified principle.
   * 
   * @param principle The principle to format with this {@link SzReportStatistic}.
   * 
   * @return The created {@link Formatter} instance.
   */
  public Formatter principle(String principle) {
    Formatter formatter = new Formatter(this);
    return formatter.principle(principle);
  }

  /**
   * Creates a {@link Formatter} that will format this {@link SzReportStatistic}
   * instance with the specified match key.
   * 
   * @param matchKey The match key to format with this {@link SzReportStatistic}.
   * 
   * @return The created {@link Formatter} instance.
   */
  public Formatter matchKey(String matchKey) {
    Formatter formatter = new Formatter(this);
    return formatter.matchKey(matchKey);
  }
}
