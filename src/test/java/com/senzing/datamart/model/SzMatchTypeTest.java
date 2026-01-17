package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.SAME_THREAD)
class SzMatchTypeTest {

    @Test
    void testAmbiguousMatchCode() {
        assertEquals("AM", SzMatchType.AMBIGUOUS_MATCH.getCode());
    }

    @Test
    void testPossibleMatchCode() {
        assertEquals("PM", SzMatchType.POSSIBLE_MATCH.getCode());
    }

    @Test
    void testDisclosedRelationCode() {
        assertEquals("DR", SzMatchType.DISCLOSED_RELATION.getCode());
    }

    @Test
    void testPossibleRelationCode() {
        assertEquals("PR", SzMatchType.POSSIBLE_RELATION.getCode());
    }

    @Test
    void testValuesContainsAllExpected() {
        SzMatchType[] values = SzMatchType.values();
        assertEquals(4, values.length);
    }

    @ParameterizedTest
    @EnumSource(SzMatchType.class)
    void testAllValuesHaveCodes(SzMatchType matchType) {
        assertNotNull(matchType.getCode());
        assertEquals(2, matchType.getCode().length());
    }

    @Test
    void testLookupAmbiguousMatch() {
        assertEquals(SzMatchType.AMBIGUOUS_MATCH, SzMatchType.lookup("AM"));
    }

    @Test
    void testLookupPossibleMatch() {
        assertEquals(SzMatchType.POSSIBLE_MATCH, SzMatchType.lookup("PM"));
    }

    @Test
    void testLookupDisclosedRelation() {
        assertEquals(SzMatchType.DISCLOSED_RELATION, SzMatchType.lookup("DR"));
    }

    @Test
    void testLookupPossibleRelation() {
        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.lookup("PR"));
    }

    @Test
    void testLookupCaseInsensitive() {
        assertEquals(SzMatchType.AMBIGUOUS_MATCH, SzMatchType.lookup("am"));
        assertEquals(SzMatchType.POSSIBLE_MATCH, SzMatchType.lookup("pm"));
        assertEquals(SzMatchType.DISCLOSED_RELATION, SzMatchType.lookup("dr"));
        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.lookup("pr"));
    }

    @Test
    void testLookupMixedCase() {
        assertEquals(SzMatchType.AMBIGUOUS_MATCH, SzMatchType.lookup("Am"));
        assertEquals(SzMatchType.POSSIBLE_MATCH, SzMatchType.lookup("Pm"));
    }

    @Test
    void testLookupWithUnknownCode() {
        assertNull(SzMatchType.lookup("XX"));
    }

    @ParameterizedTest
    @EnumSource(SzMatchType.class)
    void testLookupRoundTrip(SzMatchType matchType) {
        String code = matchType.getCode();
        SzMatchType lookedUp = SzMatchType.lookup(code);
        assertEquals(matchType, lookedUp);
    }

    @ParameterizedTest
    @EnumSource(SzMatchType.class)
    void testValueOf(SzMatchType matchType) {
        assertEquals(matchType, SzMatchType.valueOf(matchType.name()));
    }

    @Test
    void testValueOfWithInvalidName() {
        assertThrows(IllegalArgumentException.class, () ->
            SzMatchType.valueOf("INVALID"));
    }

    @Test
    void testCodesAreUnique() {
        SzMatchType[] values = SzMatchType.values();
        java.util.Set<String> codes = new java.util.HashSet<>();
        for (SzMatchType type : values) {
            assertTrue(codes.add(type.getCode()),
                "Duplicate code found: " + type.getCode());
        }
    }

    // detect() tests
    @Test
    void testDetectAmbiguousMatch() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_AMBIGUOUS", 1);
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
        JsonObject json = builder.build();

        assertEquals(SzMatchType.AMBIGUOUS_MATCH, SzMatchType.detect(json));
    }

    @Test
    void testDetectAmbiguousMatchWithZero() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_AMBIGUOUS", 0);
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
        JsonObject json = builder.build();

        // Should not be ambiguous match when IS_AMBIGUOUS is 0
        assertEquals(SzMatchType.POSSIBLE_MATCH, SzMatchType.detect(json));
    }

    @Test
    void testDetectDisclosedRelation() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_DISCLOSED", 1);
        JsonObject json = builder.build();

        assertEquals(SzMatchType.DISCLOSED_RELATION, SzMatchType.detect(json));
    }

    @Test
    void testDetectDisclosedRelationWithZero() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_DISCLOSED", 0);
        JsonObject json = builder.build();

        // Should default to POSSIBLE_RELATION when IS_DISCLOSED is 0
        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.detect(json));
    }

    @Test
    void testDetectPossibleMatch() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
        JsonObject json = builder.build();

        assertEquals(SzMatchType.POSSIBLE_MATCH, SzMatchType.detect(json));
    }

    @Test
    void testDetectPossibleRelationFromPossiblyRelated() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_RELATED");
        JsonObject json = builder.build();

        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.detect(json));
    }

    @Test
    void testDetectPossibleRelationFromNameOnly() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("MATCH_LEVEL_CODE", "NAME_ONLY");
        JsonObject json = builder.build();

        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.detect(json));
    }

    @Test
    void testDetectDefaultsToPossibleRelation() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        JsonObject json = builder.build();

        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.detect(json));
    }

    @Test
    void testDetectWithUnknownMatchLevelCode() throws Exception {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("MATCH_LEVEL_CODE", "UNKNOWN_CODE");
        JsonObject json = builder.build();

        // Capture stderr output for this test that triggers a warning log
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // Should default to POSSIBLE_RELATION for unknown codes
            assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.detect(json));
        });

        // Verify the expected warning was logged
        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Unrecognized MATCH_LEVEL_CODE value: UNKNOWN_CODE"),
                "Should log warning about unrecognized MATCH_LEVEL_CODE");
    }

    @Test
    void testDetectAmbiguousTakesPrecedence() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_AMBIGUOUS", 1);
        builder.add("IS_DISCLOSED", 1);
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
        JsonObject json = builder.build();

        // IS_AMBIGUOUS should be checked first
        assertEquals(SzMatchType.AMBIGUOUS_MATCH, SzMatchType.detect(json));
    }

    @Test
    void testDetectDisclosedAfterAmbiguous() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_AMBIGUOUS", 0);
        builder.add("IS_DISCLOSED", 1);
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
        JsonObject json = builder.build();

        // IS_DISCLOSED should be checked after IS_AMBIGUOUS
        assertEquals(SzMatchType.DISCLOSED_RELATION, SzMatchType.detect(json));
    }

    @Test
    void testDetectWithNullMatchLevelCode() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("IS_AMBIGUOUS", 0);
        builder.add("IS_DISCLOSED", 0);
        JsonObject json = builder.build();

        assertEquals(SzMatchType.POSSIBLE_RELATION, SzMatchType.detect(json));
    }
}
