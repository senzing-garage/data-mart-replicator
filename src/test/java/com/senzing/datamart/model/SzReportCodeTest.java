package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

class SzReportCodeTest {

    @Test
    void testDataSourceSummaryCode() {
        assertEquals("DSS", SzReportCode.DATA_SOURCE_SUMMARY.getCode());
    }

    @Test
    void testCrossSourceSummaryCode() {
        assertEquals("CSS", SzReportCode.CROSS_SOURCE_SUMMARY.getCode());
    }

    @Test
    void testEntitySizeBreakdownCode() {
        assertEquals("ESB", SzReportCode.ENTITY_SIZE_BREAKDOWN.getCode());
    }

    @Test
    void testEntityRelationBreakdownCode() {
        assertEquals("ERB", SzReportCode.ENTITY_RELATION_BREAKDOWN.getCode());
    }

    @Test
    void testLookupDataSourceSummary() {
        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, SzReportCode.lookup("DSS"));
    }

    @Test
    void testLookupCrossSourceSummary() {
        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, SzReportCode.lookup("CSS"));
    }

    @Test
    void testLookupEntitySizeBreakdown() {
        assertEquals(SzReportCode.ENTITY_SIZE_BREAKDOWN, SzReportCode.lookup("ESB"));
    }

    @Test
    void testLookupEntityRelationBreakdown() {
        assertEquals(SzReportCode.ENTITY_RELATION_BREAKDOWN, SzReportCode.lookup("ERB"));
    }

    @Test
    void testLookupWithUnknownCode() {
        assertNull(SzReportCode.lookup("UNKNOWN"));
    }

    @Test
    void testLookupWithNullCode() {
        // lookup(null) returns null from the map, doesn't throw
        assertNull(SzReportCode.lookup(null));
    }

    @Test
    void testLookupWithEmptyCode() {
        assertNull(SzReportCode.lookup(""));
    }

    @ParameterizedTest
    @EnumSource(SzReportCode.class)
    void testAllValuesHaveCodes(SzReportCode reportCode) {
        assertNotNull(reportCode.getCode());
        assertFalse(reportCode.getCode().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(SzReportCode.class)
    void testLookupRoundTrip(SzReportCode reportCode) {
        String code = reportCode.getCode();
        SzReportCode lookedUp = SzReportCode.lookup(code);
        assertEquals(reportCode, lookedUp);
    }

    @Test
    void testValuesContainsAllExpectedCodes() {
        SzReportCode[] values = SzReportCode.values();
        assertEquals(4, values.length);
    }

    @Test
    void testValueOf() {
        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, SzReportCode.valueOf("DATA_SOURCE_SUMMARY"));
        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, SzReportCode.valueOf("CROSS_SOURCE_SUMMARY"));
        assertEquals(SzReportCode.ENTITY_SIZE_BREAKDOWN, SzReportCode.valueOf("ENTITY_SIZE_BREAKDOWN"));
        assertEquals(SzReportCode.ENTITY_RELATION_BREAKDOWN, SzReportCode.valueOf("ENTITY_RELATION_BREAKDOWN"));
    }

    @Test
    void testValueOfWithInvalidName() {
        assertThrows(IllegalArgumentException.class, () -> SzReportCode.valueOf("INVALID"));
    }

    @Test
    void testCodesAreUnique() {
        SzReportCode[] values = SzReportCode.values();
        java.util.Set<String> codes = new java.util.HashSet<>();
        for (SzReportCode code : values) {
            assertTrue(codes.add(code.getCode()),
                "Duplicate code found: " + code.getCode());
        }
    }
}
