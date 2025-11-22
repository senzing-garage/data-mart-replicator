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
     * Describes the number of entities for which two records matched for two
     * specific data sources (possibly the same data source).
     */
    MATCHED_COUNT,

    /**
     * Describes the number of entities for which there is an ambiguous match.
     */
    AMBIGUOUS_MATCH_COUNT,

    /**
     * Describes the number of related entity pairs that are possible matches for
     * two specific data sources (possibly the same data source).
     */
    POSSIBLE_MATCH_COUNT,

    /**
     * Describes the number of related entity pairs that are related by disclosed
     * relationships for two specific data sources (possibly the same data source).
     */
    DISCLOSED_RELATION_COUNT,

    /**
     * Describes the number of related entity pairs that are related by possible
     * relationships for two specific data sources (possibly the same data source).
     */
    POSSIBLE_RELATION_COUNT;

    /**
     * Provides a formatter to format {@link SzReportStatistic} instances with
     * optional principles and match keys.
     */
    public static class Formatter {
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
            if (principle != null) {
                principle = principle.trim();
                if (principle.length() == 0)
                    principle = null;
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
            if (matchKey != null) {
                matchKey = matchKey.trim();
                if (matchKey.length() == 0)
                    matchKey = null;
            }
            this.matchKey = matchKey;
            return this;
        }

        /**
         * Gets the associated {@link SzReportStatistic}.
         * 
         * @return The associated {@link SzReportStatistic}.
         */
        public SzReportStatistic getStatistic() {
            return this.statistic;
        }

        /**
         * Gets the associated principle (if any). If no associated principle then this
         * returns <code>null</code>.
         * 
         * @return The associated principle, or <code>null</code> if none.
         */
        public String getPrinciple() {
            return this.principle;
        }

        /**
         * Gets the associated match key (if any). If no associated match key then this
         * returns <code>null</code>.
         * 
         * @return The associated match key, or <code>null</code> if none.
         */
        public String getMatchKey() {
            return this.matchKey;
        }

        /**
         * Parses an encoded statistic as an instance of {@link Formatter}. If the
         * specified parameter is <code>null</code> or an empty string after trimming
         * leading and trailing whitespace then <code>null</code> is returned.
         * 
         * @param encodedStatistic The encoded statistic.
         * 
         * @return The parsed statistic as a {@link Formatter}, or <code>null</code> if
         *         the specified parameter is <code>null</code> or empty string.
         * 
         * @throws IllegalArgumentException If the specified encoded statistic is not a
         *                                  properly formatted statistic.
         */
        public static Formatter parse(String encodedStatistic) {
            if (encodedStatistic == null)
                return null;
            String text = encodedStatistic.trim();
            if (encodedStatistic.length() == 0)
                return null;

            // get the statistic
            SzReportStatistic statistic = null;
            int index = text.indexOf(":");
            if (index < 0) {
                statistic = SzReportStatistic.valueOf(text);
                text = "";

            } else if (index == 0) {
                throw new IllegalArgumentException("Improperly formatted report statistic: " + encodedStatistic);
            } else {
                String firstToken = text.substring(0, index);
                firstToken = firstToken.trim();
                statistic = SzReportStatistic.valueOf(firstToken);
                text = (index == text.length() - 1) ? "" : text.substring(index + 1);
                text = text.trim();
            }

            // get the principle and match key
            String principle = null;
            String matchKey = null;

            index = text.indexOf(":");
            if (index < 0) {
                principle = text;
                matchKey = null;

            }
            if (index >= 0) {
                principle = (index == 0) ? null : text.substring(0, index).trim();
                text = (index == text.length() - 1) ? "" : text.substring(index + 1);
                text = text.trim();
                matchKey = (text.length() == 0) ? null : text;
            }

            // create the formatter
            Formatter formatter = new Formatter(statistic);
            if (principle != null)
                formatter.principle(principle);
            if (matchKey != null)
                formatter.matchKey(matchKey);

            // return the formatter
            return formatter;
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
            String prin = this.principle == null ? "" : this.principle.trim();
            if (this.matchKey == null) {
                return this.statistic + ":" + prin;
            }
            String mkey = this.matchKey == null ? "" : this.matchKey.trim();
            return this.statistic + ":" + prin + ":" + mkey;
        }

        /**
         * Overridden to return the result from {@link #format()}.
         */
        public String toString() {
            return this.format();
        }
    }

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
