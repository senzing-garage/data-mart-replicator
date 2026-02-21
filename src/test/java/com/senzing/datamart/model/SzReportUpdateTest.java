package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzReportUpdateTest {

    @Test
    void testConstructorWithReportKeyAndEntityId() {
        SzReportKey reportKey = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        SzReportUpdate update = new SzReportUpdate(reportKey, 100L);

        assertEquals(reportKey, update.getReportKey());
        assertEquals(100L, update.getEntityId());
        assertNull(update.getRelatedEntityId());
        assertEquals(0, update.getEntityDelta());
        assertEquals(0, update.getRecordDelta());
        assertEquals(0, update.getRelationDelta());
    }

    @Test
    void testConstructorWithReportKeyEntityIdAndRelatedId() {
        SzReportKey reportKey = new SzReportKey(SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT");
        SzReportUpdate update = new SzReportUpdate(reportKey, 100L, 200L);

        assertEquals(reportKey, update.getReportKey());
        assertEquals(100L, update.getEntityId());
        assertEquals(Long.valueOf(200L), update.getRelatedEntityId());
    }

    @Test
    void testConstructorWithReportCodeStatisticAndEntityId() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, update.getReportKey().getReportCode());
        assertEquals("ENTITY_COUNT", update.getReportKey().getStatistic());
        assertEquals(100L, update.getEntityId());
        assertNull(update.getRelatedEntityId());
    }

    @Test
    void testConstructorWithDataSourcesAndEntityId() {
        SzReportUpdate update = new SzReportUpdate(
            SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "DS1", "DS2", 100L);

        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, update.getReportKey().getReportCode());
        assertEquals("MATCHED_COUNT", update.getReportKey().getStatistic());
        assertEquals("DS1", update.getReportKey().getDataSource1());
        assertEquals("DS2", update.getReportKey().getDataSource2());
        assertEquals(100L, update.getEntityId());
        assertNull(update.getRelatedEntityId());
    }

    @Test
    void testConstructorWithReportCodeStatisticEntityIdAndRelatedId() {
        SzReportUpdate update = new SzReportUpdate(
            SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L, 200L);

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, update.getReportKey().getReportCode());
        assertEquals("ENTITY_COUNT", update.getReportKey().getStatistic());
        assertEquals(100L, update.getEntityId());
        assertEquals(Long.valueOf(200L), update.getRelatedEntityId());
    }

    @Test
    void testConstructorWithDataSourcesEntityIdAndRelatedId() {
        SzReportUpdate update = new SzReportUpdate(
            SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "DS1", "DS2", 100L, 200L);

        assertEquals(SzReportCode.CROSS_SOURCE_SUMMARY, update.getReportKey().getReportCode());
        assertEquals("MATCHED_COUNT", update.getReportKey().getStatistic());
        assertEquals("DS1", update.getReportKey().getDataSource1());
        assertEquals("DS2", update.getReportKey().getDataSource2());
        assertEquals(100L, update.getEntityId());
        assertEquals(Long.valueOf(200L), update.getRelatedEntityId());
    }

    @Test
    void testConstructorWithNullReportKeyThrows() {
        assertThrows(NullPointerException.class, () ->
            new SzReportUpdate((SzReportKey) null, 100L));
    }

    @Test
    void testConstructorWithNullReportCodeThrows() {
        assertThrows(NullPointerException.class, () ->
            new SzReportUpdate(null, "ENTITY_COUNT", 100L));
    }

    @Test
    void testConstructorWithNullStatisticThrows() {
        assertThrows(NullPointerException.class, () ->
            new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, null, 100L));
    }

    @Test
    void testSetAndGetEntityDelta() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);

        update.setEntityDelta(5);
        assertEquals(5, update.getEntityDelta());

        update.setEntityDelta(-3);
        assertEquals(-3, update.getEntityDelta());
    }

    @Test
    void testSetAndGetRecordDelta() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);

        update.setRecordDelta(10);
        assertEquals(10, update.getRecordDelta());

        update.setRecordDelta(-7);
        assertEquals(-7, update.getRecordDelta());
    }

    @Test
    void testSetAndGetRelationDelta() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);

        update.setRelationDelta(15);
        assertEquals(15, update.getRelationDelta());

        update.setRelationDelta(-12);
        assertEquals(-12, update.getRelationDelta());
    }

    @Test
    void testToString() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);
        update.setEntityDelta(5);
        update.setRecordDelta(10);
        update.setRelationDelta(15);

        String result = update.toString();

        assertNotNull(result);
        assertTrue(result.contains("SzReportUpdate"));
        assertTrue(result.contains("reportKey"));
        assertTrue(result.contains("entityDelta"));
        assertTrue(result.contains("recordDelta"));
        assertTrue(result.contains("relationDelta"));
        assertTrue(result.contains("entityId"));
        assertTrue(result.contains("100"));
    }

    @Test
    void testToStringWithRelatedId() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L, 200L);

        String result = update.toString();

        assertTrue(result.contains("relatedId"));
        assertTrue(result.contains("200"));
    }

    @Test
    void testToStringWithNullRelatedId() {
        SzReportUpdate update = new SzReportUpdate(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);

        String result = update.toString();

        assertTrue(result.contains("relatedId=[ null ]"));
    }

    // Builder tests
    @Test
    void testBuilderWithReportKeyAndEntityId() {
        SzReportKey reportKey = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        SzReportUpdate update = SzReportUpdate.builder(reportKey, 100L).build();

        assertEquals(reportKey, update.getReportKey());
        assertEquals(100L, update.getEntityId());
        assertNull(update.getRelatedEntityId());
    }

    @Test
    void testBuilderWithReportKeyEntityIdAndRelatedId() {
        SzReportKey reportKey = new SzReportKey(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT");
        SzReportUpdate update = SzReportUpdate.builder(reportKey, 100L, 200L).build();

        assertEquals(reportKey, update.getReportKey());
        assertEquals(100L, update.getEntityId());
        assertEquals(Long.valueOf(200L), update.getRelatedEntityId());
    }

    @Test
    void testBuilderWithReportCodeStatisticAndEntityId() {
        SzReportUpdate update = SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L).build();

        assertEquals(SzReportCode.DATA_SOURCE_SUMMARY, update.getReportKey().getReportCode());
        assertEquals("ENTITY_COUNT", update.getReportKey().getStatistic());
        assertEquals(100L, update.getEntityId());
    }

    @Test
    void testBuilderWithDataSourcesAndEntityId() {
        SzReportUpdate update = SzReportUpdate.builder(
            SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "DS1", "DS2", 100L).build();

        assertEquals("DS1", update.getReportKey().getDataSource1());
        assertEquals("DS2", update.getReportKey().getDataSource2());
    }

    @Test
    void testBuilderWithReportCodeStatisticEntityIdAndRelatedId() {
        SzReportUpdate update = SzReportUpdate.builder(
            SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L, 200L).build();

        assertEquals(100L, update.getEntityId());
        assertEquals(Long.valueOf(200L), update.getRelatedEntityId());
    }

    @Test
    void testBuilderWithDataSourcesEntityIdAndRelatedId() {
        SzReportUpdate update = SzReportUpdate.builder(
            SzReportCode.CROSS_SOURCE_SUMMARY, "MATCHED_COUNT", "DS1", "DS2", 100L, 200L).build();

        assertEquals(100L, update.getEntityId());
        assertEquals(Long.valueOf(200L), update.getRelatedEntityId());
    }

    @Test
    void testBuilderWithNullReportKeyThrows() {
        assertThrows(NullPointerException.class, () ->
            SzReportUpdate.builder((SzReportKey) null, 100L));
    }

    @Test
    void testBuilderWithNullReportCodeThrows() {
        assertThrows(NullPointerException.class, () ->
            SzReportUpdate.builder(null, "ENTITY_COUNT", 100L));
    }

    @Test
    void testBuilderWithNullStatisticThrows() {
        assertThrows(NullPointerException.class, () ->
            SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, null, 100L));
    }

    @Test
    void testBuilderEntities() {
        SzReportUpdate update = SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L)
            .entities(5)
            .build();

        assertEquals(5, update.getEntityDelta());
    }

    @Test
    void testBuilderRecords() {
        SzReportUpdate update = SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L)
            .records(10)
            .build();

        assertEquals(10, update.getRecordDelta());
    }

    @Test
    void testBuilderRelations() {
        SzReportUpdate update = SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L)
            .relations(15)
            .build();

        assertEquals(15, update.getRelationDelta());
    }

    @Test
    void testBuilderChaining() {
        SzReportUpdate update = SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L)
            .entities(5)
            .records(10)
            .relations(15)
            .build();

        assertEquals(5, update.getEntityDelta());
        assertEquals(10, update.getRecordDelta());
        assertEquals(15, update.getRelationDelta());
    }

    @Test
    void testBuilderWithObjectStatistic() {
        SzReportUpdate update = SzReportUpdate.builder(
            SzReportCode.ENTITY_SIZE_BREAKDOWN, Integer.valueOf(5), 100L).build();

        assertEquals("5", update.getReportKey().getStatistic());
    }

    @Test
    void testBuilderWithSzReportStatistic() {
        SzReportUpdate update = SzReportUpdate.builder(
            SzReportCode.DATA_SOURCE_SUMMARY, SzReportStatistic.ENTITY_COUNT, 100L).build();

        assertEquals("ENTITY_COUNT", update.getReportKey().getStatistic());
    }

    @Test
    void testBuilderReturnsNewInstanceEachTime() {
        SzReportUpdate.Builder builder = SzReportUpdate.builder(SzReportCode.DATA_SOURCE_SUMMARY, "ENTITY_COUNT", 100L);

        SzReportUpdate update1 = builder.entities(5).build();
        SzReportUpdate update2 = builder.entities(10).build();

        // Both should reflect the builder's current state at build time
        // but they should be separate instances
        assertNotSame(update1, update2);
    }
}
