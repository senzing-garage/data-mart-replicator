package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzReportRecordTest {

    @Test
    void testDefaultConstructor() {
        SzReportRecord record = new SzReportRecord();

        assertNull(record.getDataSource());
        assertNull(record.getRecordId());
        assertNull(record.getMatchKey());
        assertNull(record.getPrinciple());
    }

    @Test
    void testConstructorWithDataSourceAndRecordId() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");

        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("REC-001", record.getRecordId());
        assertNull(record.getMatchKey());
        assertNull(record.getPrinciple());
    }

    @Test
    void testConstructorWithNullDataSource() {
        SzReportRecord record = new SzReportRecord(null, "REC-001");

        assertNull(record.getDataSource());
        assertEquals("REC-001", record.getRecordId());
    }

    @Test
    void testConstructorWithNullRecordId() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", null);

        assertEquals("CUSTOMERS", record.getDataSource());
        assertNull(record.getRecordId());
    }

    @Test
    void testSetAndGetDataSource() {
        SzReportRecord record = new SzReportRecord();
        record.setDataSource("VENDORS");

        assertEquals("VENDORS", record.getDataSource());
    }

    @Test
    void testSetAndGetRecordId() {
        SzReportRecord record = new SzReportRecord();
        record.setRecordId("REC-002");

        assertEquals("REC-002", record.getRecordId());
    }

    @Test
    void testSetAndGetMatchKey() {
        SzReportRecord record = new SzReportRecord();
        record.setMatchKey("NAME+DOB");

        assertEquals("NAME+DOB", record.getMatchKey());
    }

    @Test
    void testSetAndGetPrinciple() {
        SzReportRecord record = new SzReportRecord();
        record.setPrinciple("MFF");

        assertEquals("MFF", record.getPrinciple());
    }

    @Test
    void testEqualsWithSameReference() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");
        assertEquals(record, record);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        record1.setMatchKey("NAME+DOB");
        record1.setPrinciple("MFF");

        SzReportRecord record2 = new SzReportRecord("CUSTOMERS", "REC-001");
        record2.setMatchKey("NAME+DOB");
        record2.setPrinciple("MFF");

        assertEquals(record1, record2);
        assertEquals(record2, record1);
    }

    @Test
    void testEqualsWithDifferentDataSource() {
        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        SzReportRecord record2 = new SzReportRecord("VENDORS", "REC-001");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithDifferentRecordId() {
        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        SzReportRecord record2 = new SzReportRecord("CUSTOMERS", "REC-002");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        record1.setMatchKey("NAME+DOB");

        SzReportRecord record2 = new SzReportRecord("CUSTOMERS", "REC-001");
        record2.setMatchKey("ADDRESS");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        record1.setPrinciple("MFF");

        SzReportRecord record2 = new SzReportRecord("CUSTOMERS", "REC-001");
        record2.setPrinciple("MFS");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithNull() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");
        assertNotEquals(null, record);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");
        assertNotEquals(record, "not a record");
    }

    @Test
    void testHashCodeConsistency() {
        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        record1.setMatchKey("NAME+DOB");
        record1.setPrinciple("MFF");

        SzReportRecord record2 = new SzReportRecord("CUSTOMERS", "REC-001");
        record2.setMatchKey("NAME+DOB");
        record2.setPrinciple("MFF");

        assertEquals(record1.hashCode(), record2.hashCode());
    }

    @Test
    void testToString() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");
        record.setMatchKey("NAME+DOB");
        record.setPrinciple("MFF");

        String result = record.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("REC-001"));
        assertTrue(result.contains("NAME+DOB"));
        assertTrue(result.contains("MFF"));
    }

    @Test
    void testToStringWithNullValues() {
        SzReportRecord record = new SzReportRecord();

        assertDoesNotThrow(() -> record.toString());
    }

    @Test
    void testSerializable() {
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");
        assertTrue(record instanceof java.io.Serializable);
    }
}
