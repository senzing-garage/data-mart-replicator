package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzReportEntityTest {

    @Test
    void testDefaultConstructor() {
        SzReportEntity entity = new SzReportEntity();

        assertEquals(0L, entity.getEntityId());
        assertNull(entity.getEntityName());
        assertNull(entity.getRecordCount());
        assertNull(entity.getRelationCount());
        assertNotNull(entity.getRecords());
        assertTrue(entity.getRecords().isEmpty());
    }

    @Test
    void testConstructorWithEntityId() {
        SzReportEntity entity = new SzReportEntity(100L);

        assertEquals(100L, entity.getEntityId());
        assertNull(entity.getEntityName());
    }

    @Test
    void testConstructorWithEntityIdAndName() {
        SzReportEntity entity = new SzReportEntity(100L, "John Doe");

        assertEquals(100L, entity.getEntityId());
        assertEquals("John Doe", entity.getEntityName());
    }

    @Test
    void testSetAndGetEntityId() {
        SzReportEntity entity = new SzReportEntity();
        entity.setEntityId(200L);

        assertEquals(200L, entity.getEntityId());
    }

    @Test
    void testSetAndGetEntityName() {
        SzReportEntity entity = new SzReportEntity();
        entity.setEntityName("Jane Doe");

        assertEquals("Jane Doe", entity.getEntityName());
    }

    @Test
    void testSetAndGetRecordCount() {
        SzReportEntity entity = new SzReportEntity();
        entity.setRecordCount(5);

        assertEquals(Integer.valueOf(5), entity.getRecordCount());
    }

    @Test
    void testSetAndGetRelationCount() {
        SzReportEntity entity = new SzReportEntity();
        entity.setRelationCount(3);

        assertEquals(Integer.valueOf(3), entity.getRelationCount());
    }

    @Test
    void testAddRecord() {
        SzReportEntity entity = new SzReportEntity(100L);
        SzReportRecord record = new SzReportRecord("CUSTOMERS", "REC-001");

        entity.addRecord(record);

        List<SzReportRecord> records = entity.getRecords();
        assertEquals(1, records.size());
        assertEquals("CUSTOMERS", records.get(0).getDataSource());
        assertEquals("REC-001", records.get(0).getRecordId());
    }

    @Test
    void testAddRecordReplacesExisting() {
        SzReportEntity entity = new SzReportEntity(100L);

        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        record1.setMatchKey("OLD");
        entity.addRecord(record1);

        SzReportRecord record2 = new SzReportRecord("CUSTOMERS", "REC-001");
        record2.setMatchKey("NEW");
        entity.addRecord(record2);

        List<SzReportRecord> records = entity.getRecords();
        assertEquals(1, records.size());
        assertEquals("NEW", records.get(0).getMatchKey());
    }

    @Test
    void testSetRecords() {
        SzReportEntity entity = new SzReportEntity(100L);

        SzReportRecord record1 = new SzReportRecord("CUSTOMERS", "REC-001");
        SzReportRecord record2 = new SzReportRecord("VENDORS", "REC-002");

        entity.setRecords(Arrays.asList(record1, record2));

        assertEquals(2, entity.getRecords().size());
    }

    @Test
    void testSetRecordsWithNull() {
        SzReportEntity entity = new SzReportEntity(100L);
        entity.addRecord(new SzReportRecord("CUSTOMERS", "REC-001"));

        entity.setRecords(null);

        assertTrue(entity.getRecords().isEmpty());
    }

    @Test
    void testRemoveRecord() {
        SzReportEntity entity = new SzReportEntity(100L);
        entity.addRecord(new SzReportRecord("CUSTOMERS", "REC-001"));
        entity.addRecord(new SzReportRecord("VENDORS", "REC-002"));

        entity.removeRecord("CUSTOMERS", "REC-001");

        List<SzReportRecord> records = entity.getRecords();
        assertEquals(1, records.size());
        assertEquals("VENDORS", records.get(0).getDataSource());
    }

    @Test
    void testRemoveAllRecords() {
        SzReportEntity entity = new SzReportEntity(100L);
        entity.addRecord(new SzReportRecord("CUSTOMERS", "REC-001"));
        entity.addRecord(new SzReportRecord("VENDORS", "REC-002"));

        entity.removeAllRecords();

        assertTrue(entity.getRecords().isEmpty());
    }

    @Test
    void testEqualsWithSameReference() {
        SzReportEntity entity = new SzReportEntity(100L, "Test");
        assertEquals(entity, entity);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzReportEntity entity1 = new SzReportEntity(100L, "Test");
        entity1.setRecordCount(5);
        entity1.setRelationCount(3);

        SzReportEntity entity2 = new SzReportEntity(100L, "Test");
        entity2.setRecordCount(5);
        entity2.setRelationCount(3);

        assertEquals(entity1, entity2);
        assertEquals(entity2, entity1);
    }

    @Test
    void testEqualsWithDifferentEntityId() {
        SzReportEntity entity1 = new SzReportEntity(100L, "Test");
        SzReportEntity entity2 = new SzReportEntity(200L, "Test");

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentEntityName() {
        SzReportEntity entity1 = new SzReportEntity(100L, "Name1");
        SzReportEntity entity2 = new SzReportEntity(100L, "Name2");

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentRecordCount() {
        SzReportEntity entity1 = new SzReportEntity(100L);
        entity1.setRecordCount(5);

        SzReportEntity entity2 = new SzReportEntity(100L);
        entity2.setRecordCount(10);

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentRelationCount() {
        SzReportEntity entity1 = new SzReportEntity(100L);
        entity1.setRelationCount(3);

        SzReportEntity entity2 = new SzReportEntity(100L);
        entity2.setRelationCount(5);

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentRecords() {
        SzReportEntity entity1 = new SzReportEntity(100L);
        entity1.addRecord(new SzReportRecord("CUSTOMERS", "REC-001"));

        SzReportEntity entity2 = new SzReportEntity(100L);
        entity2.addRecord(new SzReportRecord("VENDORS", "REC-001"));

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithNull() {
        SzReportEntity entity = new SzReportEntity(100L);
        assertNotEquals(null, entity);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzReportEntity entity = new SzReportEntity(100L);
        assertNotEquals(entity, "not an entity");
    }

    @Test
    void testHashCodeConsistency() {
        SzReportEntity entity1 = new SzReportEntity(100L, "Test");
        entity1.setRecordCount(5);
        entity1.setRelationCount(3);

        SzReportEntity entity2 = new SzReportEntity(100L, "Test");
        entity2.setRecordCount(5);
        entity2.setRelationCount(3);

        assertEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    void testToString() {
        SzReportEntity entity = new SzReportEntity(100L, "John Doe");
        entity.setRecordCount(5);
        entity.setRelationCount(3);

        String result = entity.toString();

        assertNotNull(result);
        assertTrue(result.contains("100"));
        assertTrue(result.contains("John Doe"));
        assertTrue(result.contains("5"));
        assertTrue(result.contains("3"));
    }

    @Test
    void testToStringWithNullValues() {
        SzReportEntity entity = new SzReportEntity();

        assertDoesNotThrow(() -> entity.toString());
    }

    @Test
    void testSerializable() {
        SzReportEntity entity = new SzReportEntity(100L);
        assertTrue(entity instanceof java.io.Serializable);
    }
}
