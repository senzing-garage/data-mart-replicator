package com.senzing.datamart.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enumerates the various report codes for the data mart.
 */
public enum SzReportCode {
    /**
     * The report code for the data source summary report.
     */
    DATA_SOURCE_SUMMARY("DSS"),

    /**
     * The report code for the cross-source summary report.
     */
    CROSS_SOURCE_SUMMARY("CSS"),

    /**
     * The report code for the entity size breakdown.
     *
     */
    ENTITY_SIZE_BREAKDOWN("ESB"),

    /**
     * The report code for the entity relation breakdown.
     */
    ENTITY_RELATION_BREAKDOWN("ERB");

    /**
     * The short code for the report.
     */
    private String code;

    /**
     * Constructs with the short text code.
     *
     * @param code The short text code for the report.
     */
    SzReportCode(String code) {
        this.code = code;
    }

    /**
     * Returns the short text code for this instance.
     *
     * @return The short text code for this instance.
     */
    public String getCode() {
        return this.code;
    }

    /**
     * Looks up the {@link SzReportCode} for the specified text code. This returns
     * <code>null</code> if the specified code is not recognized.
     *
     * @param code The code for which the {@link SzReportCode} is being requested.
     * @return The {@link SzReportCode} for the specified code, or <code>null</code>
     *         if the specified code was not recognized.
     */
    public static SzReportCode lookup(String code) {
        return CODE_LOOKUP_MAP.get(code);
    }

    /**
     * The {@link Map} of {@link String} text report code keys to
     * {@link SzReportCode} values.
     */
    private static final Map<String, SzReportCode> CODE_LOOKUP_MAP;

    /**
     * Initializes the code lookup map.
     */
    static {
        SzReportCode[] reportCodes = SzReportCode.values();
        Map<String, SzReportCode> map = new LinkedHashMap<>();
        for (SzReportCode reportCode : reportCodes) {
            map.put(reportCode.getCode(), reportCode);
        }
        CODE_LOOKUP_MAP = Collections.unmodifiableMap(map);
    }
}
