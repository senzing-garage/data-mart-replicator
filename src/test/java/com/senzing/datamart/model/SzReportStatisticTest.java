package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

class SzReportStatisticTest {

    @Test
    void testEntityCountValue() {
        assertEquals("ENTITY_COUNT", SzReportStatistic.ENTITY_COUNT.toString());
    }

    @Test
    void testUnmatchedCountValue() {
        assertEquals("UNMATCHED_COUNT", SzReportStatistic.UNMATCHED_COUNT.toString());
    }

    @Test
    void testMatchedCountValue() {
        assertEquals("MATCHED_COUNT", SzReportStatistic.MATCHED_COUNT.toString());
    }

    @Test
    void testAmbiguousMatchCountValue() {
        assertEquals("AMBIGUOUS_MATCH_COUNT", SzReportStatistic.AMBIGUOUS_MATCH_COUNT.toString());
    }

    @Test
    void testPossibleMatchCountValue() {
        assertEquals("POSSIBLE_MATCH_COUNT", SzReportStatistic.POSSIBLE_MATCH_COUNT.toString());
    }

    @Test
    void testDisclosedRelationCountValue() {
        assertEquals("DISCLOSED_RELATION_COUNT", SzReportStatistic.DISCLOSED_RELATION_COUNT.toString());
    }

    @Test
    void testPossibleRelationCountValue() {
        assertEquals("POSSIBLE_RELATION_COUNT", SzReportStatistic.POSSIBLE_RELATION_COUNT.toString());
    }

    @ParameterizedTest
    @EnumSource(SzReportStatistic.class)
    void testValueOf(SzReportStatistic statistic) {
        assertEquals(statistic, SzReportStatistic.valueOf(statistic.name()));
    }

    @Test
    void testValuesContainsAllExpected() {
        SzReportStatistic[] values = SzReportStatistic.values();
        assertEquals(7, values.length);
    }

    @Test
    void testPrincipleCreatesFormatter() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle("MFF");

        assertNotNull(formatter);
        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertEquals("MFF", formatter.getPrinciple());
        assertNull(formatter.getMatchKey());
    }

    @Test
    void testMatchKeyCreatesFormatter() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.matchKey("NAME+DOB");

        assertNotNull(formatter);
        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertNull(formatter.getPrinciple());
        assertEquals("NAME+DOB", formatter.getMatchKey());
    }

    // Formatter tests
    @Test
    void testFormatterPrinciple() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle("MFF");

        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertEquals("MFF", formatter.getPrinciple());
    }

    @Test
    void testFormatterPrincipleWithNull() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle(null);

        assertNull(formatter.getPrinciple());
    }

    @Test
    void testFormatterPrincipleWithEmptyString() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle("   ");

        assertNull(formatter.getPrinciple());
    }

    @Test
    void testFormatterPrincipleTrims() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle("  MFF  ");

        assertEquals("MFF", formatter.getPrinciple());
    }

    @Test
    void testFormatterMatchKey() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.matchKey("NAME+DOB");

        assertEquals("NAME+DOB", formatter.getMatchKey());
    }

    @Test
    void testFormatterMatchKeyWithNull() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.matchKey(null);

        assertNull(formatter.getMatchKey());
    }

    @Test
    void testFormatterMatchKeyWithEmptyString() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.matchKey("   ");

        assertNull(formatter.getMatchKey());
    }

    @Test
    void testFormatterMatchKeyTrims() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.matchKey("  NAME+DOB  ");

        assertEquals("NAME+DOB", formatter.getMatchKey());
    }

    @Test
    void testFormatterChaining() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT
            .principle("MFF")
            .matchKey("NAME+DOB");

        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertEquals("MFF", formatter.getPrinciple());
        assertEquals("NAME+DOB", formatter.getMatchKey());
    }

    @Test
    void testFormatterFormatWithNoExtras() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle(null);
        formatter.matchKey(null);

        assertEquals("ENTITY_COUNT", formatter.format());
    }

    @Test
    void testFormatterFormatWithPrincipleOnly() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT.principle("MFF");

        assertEquals("ENTITY_COUNT:MFF", formatter.format());
    }

    @Test
    void testFormatterFormatWithBoth() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT
            .principle("MFF")
            .matchKey("NAME+DOB");

        assertEquals("ENTITY_COUNT:MFF:NAME+DOB", formatter.format());
    }

    @Test
    void testFormatterFormatWithMatchKeyOnly() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT
            .matchKey("NAME+DOB");

        assertEquals("ENTITY_COUNT::NAME+DOB", formatter.format());
    }

    @Test
    void testFormatterToString() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.ENTITY_COUNT
            .principle("MFF")
            .matchKey("NAME+DOB");

        assertEquals(formatter.format(), formatter.toString());
    }

    @Test
    void testFormatterParseSimple() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.Formatter.parse("ENTITY_COUNT");

        assertNotNull(formatter);
        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertNull(formatter.getPrinciple());
        assertNull(formatter.getMatchKey());
    }

    @Test
    void testFormatterParseWithPrinciple() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.Formatter.parse("ENTITY_COUNT:MFF");

        assertNotNull(formatter);
        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertEquals("MFF", formatter.getPrinciple());
        assertNull(formatter.getMatchKey());
    }

    @Test
    void testFormatterParseWithPrincipleAndMatchKey() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.Formatter.parse("ENTITY_COUNT:MFF:NAME+DOB");

        assertNotNull(formatter);
        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertEquals("MFF", formatter.getPrinciple());
        assertEquals("NAME+DOB", formatter.getMatchKey());
    }

    @Test
    void testFormatterParseWithEmptyPrinciple() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.Formatter.parse("ENTITY_COUNT::NAME+DOB");

        assertNotNull(formatter);
        assertEquals(SzReportStatistic.ENTITY_COUNT, formatter.getStatistic());
        assertNull(formatter.getPrinciple());
        assertEquals("NAME+DOB", formatter.getMatchKey());
    }

    @Test
    void testFormatterParseWithNull() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.Formatter.parse(null);
        assertNull(formatter);
    }

    @Test
    void testFormatterParseWithEmptyString() {
        SzReportStatistic.Formatter formatter = SzReportStatistic.Formatter.parse("");
        assertNull(formatter);
    }

    @Test
    void testFormatterParseWithInvalidStatistic() {
        assertThrows(IllegalArgumentException.class, () ->
            SzReportStatistic.Formatter.parse("INVALID_STATISTIC"));
    }

    @Test
    void testFormatterParseWithLeadingColon() {
        assertThrows(IllegalArgumentException.class, () ->
            SzReportStatistic.Formatter.parse(":ENTITY_COUNT"));
    }

    @Test
    void testFormatterRoundTrip() {
        SzReportStatistic.Formatter original = SzReportStatistic.MATCHED_COUNT
            .principle("MFF")
            .matchKey("NAME+DOB");

        String formatted = original.format();
        SzReportStatistic.Formatter parsed = SzReportStatistic.Formatter.parse(formatted);

        assertEquals(original.getStatistic(), parsed.getStatistic());
        assertEquals(original.getPrinciple(), parsed.getPrinciple());
        assertEquals(original.getMatchKey(), parsed.getMatchKey());
    }

    @Test
    void testFormatterRoundTripNoExtras() {
        SzReportStatistic.Formatter original = SzReportStatistic.ENTITY_COUNT.principle(null);
        original.matchKey(null);

        String formatted = original.format();
        SzReportStatistic.Formatter parsed = SzReportStatistic.Formatter.parse(formatted);

        assertEquals(original.getStatistic(), parsed.getStatistic());
    }
}
