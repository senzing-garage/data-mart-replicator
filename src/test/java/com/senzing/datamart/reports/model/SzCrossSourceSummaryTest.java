package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzCrossSourceSummaryTest {

    @Test
    void testDefaultConstructor() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary();

        assertNull(summary.getDataSource());
        assertNull(summary.getVersusDataSource());
        assertNotNull(summary.getMatches());
        assertTrue(summary.getMatches().isEmpty());
        assertNotNull(summary.getAmbiguousMatches());
        assertTrue(summary.getAmbiguousMatches().isEmpty());
        assertNotNull(summary.getPossibleMatches());
        assertTrue(summary.getPossibleMatches().isEmpty());
        assertNotNull(summary.getPossibleRelations());
        assertTrue(summary.getPossibleRelations().isEmpty());
        assertNotNull(summary.getDisclosedRelations());
        assertTrue(summary.getDisclosedRelations().isEmpty());
    }

    @Test
    void testConstructorWithDataSources() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        assertEquals("CUSTOMERS", summary.getDataSource());
        assertEquals("VENDORS", summary.getVersusDataSource());
    }

    @Test
    void testSetAndGetDataSource() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary();
        summary.setDataSource("CUSTOMERS");

        assertEquals("CUSTOMERS", summary.getDataSource());
    }

    @Test
    void testSetAndGetVersusDataSource() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary();
        summary.setVersusDataSource("VENDORS");

        assertEquals("VENDORS", summary.getVersusDataSource());
    }

    // Matches tests
    @Test
    void testAddMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzMatchCounts matchCounts = createTestMatchCounts("NAME+DOB", "MFF");

        summary.addMatches(matchCounts);

        List<SzMatchCounts> result = summary.getMatches();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
    }

    @Test
    void testAddMatchesWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.addMatches(null);

        assertTrue(summary.getMatches().isEmpty());
    }

    @Test
    void testSetMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzMatchCounts counts1 = createTestMatchCounts("NAME+DOB", "MFF");
        SzMatchCounts counts2 = createTestMatchCounts("ADDRESS", "MFS");

        summary.setMatches(Arrays.asList(counts1, counts2));

        assertEquals(2, summary.getMatches().size());
    }

    @Test
    void testSetMatchesWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addMatches(createTestMatchCounts("NAME+DOB", "MFF"));

        summary.setMatches(null);

        assertTrue(summary.getMatches().isEmpty());
    }

    @Test
    void testRemoveMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addMatches(createTestMatchCounts("NAME+DOB", "MFF"));
        summary.addMatches(createTestMatchCounts("ADDRESS", "MFS"));

        summary.removeMatches("NAME+DOB", "MFF");

        assertEquals(1, summary.getMatches().size());
    }

    @Test
    void testRemoveAllMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addMatches(createTestMatchCounts("NAME+DOB", "MFF"));
        summary.addMatches(createTestMatchCounts("ADDRESS", "MFS"));

        summary.removeAllMatches();

        assertTrue(summary.getMatches().isEmpty());
    }

    // Ambiguous matches tests
    @Test
    void testAddAmbiguousMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts relationCounts = createTestRelationCounts("NAME+DOB", "MFF");

        summary.addAmbiguousMatches(relationCounts);

        List<SzRelationCounts> result = summary.getAmbiguousMatches();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
    }

    @Test
    void testAddAmbiguousMatchesWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.addAmbiguousMatches(null);

        assertTrue(summary.getAmbiguousMatches().isEmpty());
    }

    @Test
    void testSetAmbiguousMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts counts1 = createTestRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts counts2 = createTestRelationCounts("ADDRESS", "MFS");

        summary.setAmbiguousMatches(Arrays.asList(counts1, counts2));

        assertEquals(2, summary.getAmbiguousMatches().size());
    }

    @Test
    void testSetAmbiguousMatchesWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addAmbiguousMatches(createTestRelationCounts("NAME+DOB", "MFF"));

        summary.setAmbiguousMatches(null);

        assertTrue(summary.getAmbiguousMatches().isEmpty());
    }

    @Test
    void testRemoveAmbiguousMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addAmbiguousMatches(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addAmbiguousMatches(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removeAmbiguousMatches("NAME+DOB", "MFF");

        assertEquals(1, summary.getAmbiguousMatches().size());
    }

    @Test
    void testRemoveAllAmbiguousMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addAmbiguousMatches(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addAmbiguousMatches(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removeAllAmbiguousMatches();

        assertTrue(summary.getAmbiguousMatches().isEmpty());
    }

    // Possible matches tests
    @Test
    void testAddPossibleMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts relationCounts = createTestRelationCounts("NAME+DOB", "MFF");

        summary.addPossibleMatches(relationCounts);

        List<SzRelationCounts> result = summary.getPossibleMatches();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
    }

    @Test
    void testAddPossibleMatchesWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.addPossibleMatches(null);

        assertTrue(summary.getPossibleMatches().isEmpty());
    }

    @Test
    void testSetPossibleMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts counts1 = createTestRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts counts2 = createTestRelationCounts("ADDRESS", "MFS");

        summary.setPossibleMatches(Arrays.asList(counts1, counts2));

        assertEquals(2, summary.getPossibleMatches().size());
    }

    @Test
    void testSetPossibleMatchesWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addPossibleMatches(createTestRelationCounts("NAME+DOB", "MFF"));

        summary.setPossibleMatches(null);

        assertTrue(summary.getPossibleMatches().isEmpty());
    }

    @Test
    void testRemovePossibleMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addPossibleMatches(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addPossibleMatches(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removePossibleMatches("NAME+DOB", "MFF");

        assertEquals(1, summary.getPossibleMatches().size());
    }

    @Test
    void testRemoveAllPossibleMatches() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addPossibleMatches(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addPossibleMatches(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removeAllPossibleMatches();

        assertTrue(summary.getPossibleMatches().isEmpty());
    }

    // Possible relations tests
    @Test
    void testAddPossibleRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts relationCounts = createTestRelationCounts("NAME+DOB", "MFF");

        summary.addPossibleRelations(relationCounts);

        List<SzRelationCounts> result = summary.getPossibleRelations();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
    }

    @Test
    void testAddPossibleRelationsWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.addPossibleRelations(null);

        assertTrue(summary.getPossibleRelations().isEmpty());
    }

    @Test
    void testSetPossibleRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts counts1 = createTestRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts counts2 = createTestRelationCounts("ADDRESS", "MFS");

        summary.setPossibleRelations(Arrays.asList(counts1, counts2));

        assertEquals(2, summary.getPossibleRelations().size());
    }

    @Test
    void testSetPossibleRelationsWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addPossibleRelations(createTestRelationCounts("NAME+DOB", "MFF"));

        summary.setPossibleRelations(null);

        assertTrue(summary.getPossibleRelations().isEmpty());
    }

    @Test
    void testRemovePossibleRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addPossibleRelations(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addPossibleRelations(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removePossibleRelations("NAME+DOB", "MFF");

        assertEquals(1, summary.getPossibleRelations().size());
    }

    @Test
    void testRemoveAllPossibleRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addPossibleRelations(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addPossibleRelations(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removeAllPossibleRelations();

        assertTrue(summary.getPossibleRelations().isEmpty());
    }

    // Disclosed relations tests
    @Test
    void testAddDisclosedRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts relationCounts = createTestRelationCounts("NAME+DOB", "MFF");

        summary.addDisclosedRelations(relationCounts);

        List<SzRelationCounts> result = summary.getDisclosedRelations();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
    }

    @Test
    void testAddDisclosedRelationsWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.addDisclosedRelations(null);

        assertTrue(summary.getDisclosedRelations().isEmpty());
    }

    @Test
    void testSetDisclosedRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzRelationCounts counts1 = createTestRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts counts2 = createTestRelationCounts("ADDRESS", "MFS");

        summary.setDisclosedRelations(Arrays.asList(counts1, counts2));

        assertEquals(2, summary.getDisclosedRelations().size());
    }

    @Test
    void testSetDisclosedRelationsWithNull() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addDisclosedRelations(createTestRelationCounts("NAME+DOB", "MFF"));

        summary.setDisclosedRelations(null);

        assertTrue(summary.getDisclosedRelations().isEmpty());
    }

    @Test
    void testRemoveDisclosedRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addDisclosedRelations(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addDisclosedRelations(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removeDisclosedRelations("NAME+DOB", "MFF");

        assertEquals(1, summary.getDisclosedRelations().size());
    }

    @Test
    void testRemoveAllDisclosedRelations() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addDisclosedRelations(createTestRelationCounts("NAME+DOB", "MFF"));
        summary.addDisclosedRelations(createTestRelationCounts("ADDRESS", "MFS"));

        summary.removeAllDisclosedRelations();

        assertTrue(summary.getDisclosedRelations().isEmpty());
    }

    // Equals and hashCode tests
    @Test
    void testEqualsWithSameReference() {
        SzCrossSourceSummary summary = createTestCrossSourceSummary();
        assertEquals(summary, summary);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzCrossSourceSummary summary1 = createTestCrossSourceSummary();
        SzCrossSourceSummary summary2 = createTestCrossSourceSummary();

        assertEquals(summary1, summary2);
        assertEquals(summary2, summary1);
    }

    @Test
    void testEqualsWithDifferentDataSource() {
        SzCrossSourceSummary summary1 = createTestCrossSourceSummary();
        SzCrossSourceSummary summary2 = createTestCrossSourceSummary();
        summary2.setDataSource("DIFFERENT");

        assertNotEquals(summary1, summary2);
    }

    @Test
    void testEqualsWithDifferentVersusDataSource() {
        SzCrossSourceSummary summary1 = createTestCrossSourceSummary();
        SzCrossSourceSummary summary2 = createTestCrossSourceSummary();
        summary2.setVersusDataSource("DIFFERENT");

        assertNotEquals(summary1, summary2);
    }

    @Test
    void testEqualsWithDifferentMatches() {
        SzCrossSourceSummary summary1 = createTestCrossSourceSummary();
        SzCrossSourceSummary summary2 = createTestCrossSourceSummary();
        summary2.addMatches(createTestMatchCounts("EXTRA", "RULE"));

        assertNotEquals(summary1, summary2);
    }

    @Test
    void testEqualsWithNull() {
        SzCrossSourceSummary summary = createTestCrossSourceSummary();
        assertNotEquals(null, summary);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzCrossSourceSummary summary = createTestCrossSourceSummary();
        assertNotEquals(summary, "not a summary");
    }

    @Test
    void testHashCodeConsistency() {
        SzCrossSourceSummary summary1 = createTestCrossSourceSummary();
        SzCrossSourceSummary summary2 = createTestCrossSourceSummary();

        assertEquals(summary1.hashCode(), summary2.hashCode());
    }

    @Test
    void testToString() {
        SzCrossSourceSummary summary = createTestCrossSourceSummary();
        String result = summary.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("VENDORS"));
    }

    @Test
    void testToStringWithNullValues() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary();

        assertDoesNotThrow(() -> summary.toString());
    }

    @Test
    void testSerializable() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary();
        assertTrue(summary instanceof java.io.Serializable);
    }

    private SzMatchCounts createTestMatchCounts(String matchKey, String principle) {
        SzMatchCounts counts = new SzMatchCounts(matchKey, principle);
        counts.setEntityCount(100L);
        counts.setRecordCount(200L);
        return counts;
    }

    private SzRelationCounts createTestRelationCounts(String matchKey, String principle) {
        SzRelationCounts counts = new SzRelationCounts(matchKey, principle);
        counts.setEntityCount(100L);
        counts.setRecordCount(200L);
        counts.setRelationCount(50L);
        return counts;
    }

    private SzCrossSourceSummary createTestCrossSourceSummary() {
        SzCrossSourceSummary summary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        summary.addMatches(createTestMatchCounts("NAME+DOB", "MFF"));
        summary.addAmbiguousMatches(createTestRelationCounts("NAME", "MFS"));
        return summary;
    }
}
