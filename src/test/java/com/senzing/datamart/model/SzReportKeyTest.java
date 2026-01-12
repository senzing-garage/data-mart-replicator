package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzReportKeyTest {

    @Test
    void testConstructorWithCodeAndStatistic() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("ENTITY_COUNT", key.getStatistic());
        assertNull(key.getDataSource1());
        assertNull(key.getDataSource2());
    }

    @Test
    void testConstructorWithCodeAndNumberStatistic() {
        SzReportKey key = new SzReportKey(SzReportCode.ENTITY_SIZE_BREAKDOWN, 5);

        assertEquals(SzReportCode.ENTITY_SIZE_BREAKDOWN, key.getReportCode());
        assertEquals("5", key.getStatistic());
    }

    @Test
    void testConstructorWithCodeAndNullNumberStatistic() {
        SzReportKey key = new SzReportKey(SzReportCode.ENTITY_SIZE_BREAKDOWN, (Number) null);

        assertEquals(SzReportCode.ENTITY_SIZE_BREAKDOWN, key.getReportCode());
        assertNull(key.getStatistic());
    }

    @Test
    void testConstructorWithOneDataSource() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "CUSTOMERS");

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("ENTITY_COUNT", key.getStatistic());
        assertEquals("CUSTOMERS", key.getDataSource1());
        assertNull(key.getDataSource2());
    }

    @Test
    void testConstructorWithTwoDataSources() {
        SzReportKey key = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "CUSTOMERS", "VENDORS");

        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("MATCHED_COUNT", key.getStatistic());
        assertEquals("CUSTOMERS", key.getDataSource1());
        assertEquals("VENDORS", key.getDataSource2());
    }

    @Test
    void testConstructorWithSzReportStatistic() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, SzReportStatistic.ENTITY_COUNT);

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("ENTITY_COUNT", key.getStatistic());
    }

    @Test
    void testConstructorWithNullSzReportStatistic() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, (SzReportStatistic) null);

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, key.getReportCode());
        assertNull(key.getStatistic());
    }

    @Test
    void testConstructorWithSzReportStatisticAndDataSources() {
        SzReportKey key = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, SzReportStatistic.MATCHED_COUNT, "DS1", "DS2");

        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("MATCHED_COUNT", key.getStatistic());
        assertEquals("DS1", key.getDataSource1());
        assertEquals("DS2", key.getDataSource2());
    }

    @Test
    void testConstructorWithNullDataSource1AndNonNullDataSource2Throws() {
        assertThrows(IllegalArgumentException.class, () ->
            new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "STAT", null, "DS2"));
    }

    @Test
    void testEqualsWithSameReference() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        assertEquals(key, key);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzReportKey key1 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS1", "DS2");
        SzReportKey key2 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS1", "DS2");

        assertEquals(key1, key2);
        assertEquals(key2, key1);
    }

    @Test
    void testEqualsWithDifferentReportCode() {
        SzReportKey key1 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        SzReportKey key2 = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "ENTITY_COUNT");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithDifferentStatistic() {
        SzReportKey key1 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        SzReportKey key2 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "RECORD_COUNT");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithDifferentDataSource1() {
        SzReportKey key1 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS1");
        SzReportKey key2 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS2");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithDifferentDataSource2() {
        SzReportKey key1 = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "DS1", "DS2");
        SzReportKey key2 = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "DS1", "DS3");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithNull() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        assertNotEquals(null, key);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        assertNotEquals(key, "not a key");
    }

    @Test
    void testHashCodeConsistency() {
        SzReportKey key1 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS1", "DS2");
        SzReportKey key2 = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS1", "DS2");

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testToStringBasic() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        String result = key.toString();

        assertEquals("DSS:ENTITY_COUNT", result);
    }

    @Test
    void testToStringWithOneDataSource() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "CUSTOMERS");
        String result = key.toString();

        assertEquals("DSS:ENTITY_COUNT:CUSTOMERS", result);
    }

    @Test
    void testToStringWithTwoDataSources() {
        SzReportKey key = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "CUSTOMERS", "VENDORS");
        String result = key.toString();

        assertEquals("CSS:MATCHED_COUNT:CUSTOMERS:VENDORS", result);
    }

    @Test
    void testToStringUrlEncodesSpecialCharacters() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "STAT:WITH:COLONS", "DS:1");
        String result = key.toString();

        assertTrue(result.contains("%3A"));
    }

    @Test
    void testParseBasic() {
        SzReportKey key = SzReportKey.parse("DSS:ENTITY_COUNT");

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("ENTITY_COUNT", key.getStatistic());
        assertNull(key.getDataSource1());
        assertNull(key.getDataSource2());
    }

    @Test
    void testParseWithOneDataSource() {
        SzReportKey key = SzReportKey.parse("DSS:ENTITY_COUNT:CUSTOMERS");

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("ENTITY_COUNT", key.getStatistic());
        assertEquals("CUSTOMERS", key.getDataSource1());
        assertNull(key.getDataSource2());
    }

    @Test
    void testParseWithTwoDataSources() {
        SzReportKey key = SzReportKey.parse("CSS:MATCHED_COUNT:CUSTOMERS:VENDORS");

        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, key.getReportCode());
        assertEquals("MATCHED_COUNT", key.getStatistic());
        assertEquals("CUSTOMERS", key.getDataSource1());
        assertEquals("VENDORS", key.getDataSource2());
    }

    @Test
    void testParseDecodesUrlEncodedCharacters() {
        String encoded = "DSS:STAT%3AWITH%3ACOLONS:DS%3A1";
        SzReportKey key = SzReportKey.parse(encoded);

        assertEquals("STAT:WITH:COLONS", key.getStatistic());
        assertEquals("DS:1", key.getDataSource1());
    }

    @Test
    void testParseWithTooFewTokensThrows() {
        assertThrows(IllegalArgumentException.class, () -> SzReportKey.parse("DSS"));
    }

    @Test
    void testParseWithTooManyTokensThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            SzReportKey.parse("DSS:STAT:DS1:DS2:EXTRA"));
    }

    @Test
    void testRoundTripThroughToStringAndParse() {
        SzReportKey original = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "CUSTOMERS", "VENDORS");
        String encoded = original.toString();
        SzReportKey parsed = SzReportKey.parse(encoded);

        assertEquals(original, parsed);
    }

    @Test
    void testRoundTripWithSpecialCharacters() {
        SzReportKey original = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "STAT:WITH:COLONS", "DS:1", "DS:2");
        String encoded = original.toString();
        SzReportKey parsed = SzReportKey.parse(encoded);

        assertEquals(original, parsed);
    }

    @Test
    void testSerializable() {
        SzReportKey key = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", "DS1", "DS2");
        assertTrue(key instanceof java.io.Serializable);
    }
}
