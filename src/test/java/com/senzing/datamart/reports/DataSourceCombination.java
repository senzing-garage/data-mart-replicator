package com.senzing.datamart.reports;

/**
 * Enumerates the different combinations of data sources used in report tests.
 * This enum classifies the sets of data sources that can be used when testing
 * report generation, allowing tests to verify behavior with different subsets
 * of configured data sources.
 */
public enum DataSourceCombination {
    /**
     * Represents only the data sources that have loaded records.
     * When used with service interfaces, this typically corresponds to
     * {@code onlyLoaded = true}.
     */
    LOADED,

    /**
     * Represents all configured data sources, excluding the default data sources
     * (such as TEST and SEARCH). When used with service interfaces, this
     * typically corresponds to {@code onlyLoaded = false} since the service
     * will call {@code getConfiguredDataSources(true)} to exclude defaults.
     */
    ALL_BUT_DEFAULT,

    /**
     * Represents all configured data sources, including the default data sources
     * (such as TEST and SEARCH). This combination is typically only supported
     * by direct report calls, not by service interfaces.
     */
    ALL_WITH_DEFAULT,

    /**
     * Represents a complex subset of the data sources, used for testing
     * various combinations of loaded and unused data sources. This combination
     * is typically only supported by direct report calls, not by service
     * interfaces.
     */
    COMPLEX
}
