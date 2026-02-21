package com.senzing.listener.service.locking;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ResourceKey}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ResourceKeyTest {

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    @Test
    @Order(100)
    void testConstructorWithStringComponents() {
        ResourceKey key = new ResourceKey("ENTITY", "123", "456");
        assertEquals("ENTITY", key.getResourceType());
        assertEquals(List.of("123", "456"), key.getComponents());
    }

    @Test
    @Order(200)
    void testConstructorWithNoComponents() {
        ResourceKey key = new ResourceKey("ENTITY");
        assertEquals("ENTITY", key.getResourceType());
        assertTrue(key.getComponents().isEmpty());
    }

    @Test
    @Order(300)
    void testConstructorWithNullStringComponents() {
        ResourceKey key = new ResourceKey("ENTITY", (String[]) null);
        assertEquals("ENTITY", key.getResourceType());
        assertTrue(key.getComponents().isEmpty());
    }

    @Test
    @Order(400)
    void testConstructorWithObjectComponents() {
        ResourceKey key = new ResourceKey("ENTITY", 123L, "test", 456);
        assertEquals("ENTITY", key.getResourceType());
        assertEquals(List.of("123", "test", "456"), key.getComponents());
    }

    @Test
    @Order(500)
    void testConstructorWithNullObjectComponents() {
        ResourceKey key = new ResourceKey("ENTITY", (Object[]) null);
        assertEquals("ENTITY", key.getResourceType());
        assertTrue(key.getComponents().isEmpty());
    }

    @Test
    @Order(600)
    void testConstructorWithNullResourceTypeThrows() {
        assertThrows(NullPointerException.class, () -> new ResourceKey(null, "comp1"));
    }

    @Test
    @Order(700)
    void testConstructorWithNullResourceTypeObjectsThrows() {
        assertThrows(NullPointerException.class, () -> new ResourceKey(null, (Object[]) null));
    }

    // ========================================================================
    // Getter Tests
    // ========================================================================

    @Test
    @Order(800)
    void testGetResourceType() {
        ResourceKey key = new ResourceKey("RECORD", "DS1", "R001");
        assertEquals("RECORD", key.getResourceType());
    }

    @Test
    @Order(900)
    void testGetComponents() {
        ResourceKey key = new ResourceKey("RECORD", "DS1", "R001");
        List<String> components = key.getComponents();
        assertEquals(2, components.size());
        assertEquals("DS1", components.get(0));
        assertEquals("R001", components.get(1));
    }

    @Test
    @Order(1000)
    void testComponentsListIsUnmodifiable() {
        ResourceKey key = new ResourceKey("ENTITY", "123");
        List<String> components = key.getComponents();
        assertThrows(UnsupportedOperationException.class, () -> components.add("newComp"));
    }

    // ========================================================================
    // equals() and hashCode() Tests
    // ========================================================================

    @Test
    @Order(1100)
    void testEqualsWithSameInstance() {
        ResourceKey key = new ResourceKey("ENTITY", "123");
        assertEquals(key, key);
    }

    @Test
    @Order(1200)
    void testEqualsWithNull() {
        ResourceKey key = new ResourceKey("ENTITY", "123");
        // Call equals directly to ensure the null branch is covered
        assertFalse(key.equals(null));
    }

    @Test
    @Order(1300)
    void testEqualsWithDifferentClass() {
        ResourceKey key = new ResourceKey("ENTITY", "123");
        // Call equals directly to ensure the different class branch is covered
        assertFalse(key.equals("ENTITY:123"));
    }

    @Test
    @Order(1400)
    void testEqualsWithEquivalentKeys() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123", "456");
        ResourceKey key2 = new ResourceKey("ENTITY", "123", "456");
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    @Order(1500)
    void testEqualsWithDifferentResourceTypes() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("RECORD", "123");
        assertNotEquals(key1, key2);
    }

    @Test
    @Order(1600)
    void testEqualsWithDifferentComponents() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("ENTITY", "456");
        assertNotEquals(key1, key2);
    }

    @Test
    @Order(1700)
    void testEqualsWithDifferentComponentCounts() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("ENTITY", "123", "456");
        assertNotEquals(key1, key2);
    }

    @Test
    @Order(1800)
    void testHashCodeConsistency() {
        ResourceKey key = new ResourceKey("ENTITY", "123");
        int hash1 = key.hashCode();
        int hash2 = key.hashCode();
        assertEquals(hash1, hash2);
    }

    // ========================================================================
    // compareTo() Tests
    // ========================================================================

    @Test
    @Order(1900)
    void testCompareToSameKey() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("ENTITY", "123");
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    @Order(2000)
    void testCompareToDifferentResourceTypes() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("RECORD", "123");
        assertTrue(key1.compareTo(key2) < 0); // ENTITY < RECORD
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    @Order(2100)
    void testCompareToDifferentComponentCounts() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("ENTITY", "123", "456");
        assertTrue(key1.compareTo(key2) < 0); // fewer components comes first
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    @Order(2200)
    void testCompareToDifferentComponentValues() {
        ResourceKey key1 = new ResourceKey("ENTITY", "AAA");
        ResourceKey key2 = new ResourceKey("ENTITY", "BBB");
        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    @Order(2300)
    void testCompareToWithStringNullComponent() {
        // Object constructor converts null to "null" string via String.valueOf()
        // So we test comparison with "null" string
        ResourceKey key1 = new ResourceKey("ENTITY", (Object) null); // becomes "null"
        ResourceKey key2 = new ResourceKey("ENTITY", "ABC");
        // "null" compared to "ABC" - "n" > "A" so key1 > key2
        assertTrue(key1.compareTo(key2) > 0);
        assertTrue(key2.compareTo(key1) < 0);
    }

    @Test
    @Order(2400)
    void testCompareToWithBothNullStringComponents() {
        // Both will have "null" as component
        ResourceKey key1 = new ResourceKey("ENTITY", (Object) null);
        ResourceKey key2 = new ResourceKey("ENTITY", (Object) null);
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    @Order(2500)
    void testCompareToMultipleComponentsSameValues() {
        ResourceKey key1 = new ResourceKey("RECORD", "DS1", "R001");
        ResourceKey key2 = new ResourceKey("RECORD", "DS1", "R001");
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    @Order(2600)
    void testCompareToMultipleComponentsDifferent() {
        ResourceKey key1 = new ResourceKey("RECORD", "DS1", "R001");
        ResourceKey key2 = new ResourceKey("RECORD", "DS1", "R002");
        assertTrue(key1.compareTo(key2) < 0);
    }

    // ========================================================================
    // toString() Tests
    // ========================================================================

    @Test
    @Order(2700)
    void testToStringNoComponents() {
        ResourceKey key = new ResourceKey("ENTITY");
        assertEquals("ENTITY", key.toString());
    }

    @Test
    @Order(2800)
    void testToStringWithComponents() {
        ResourceKey key = new ResourceKey("ENTITY", "123");
        assertEquals("ENTITY:123", key.toString());
    }

    @Test
    @Order(2900)
    void testToStringWithMultipleComponents() {
        ResourceKey key = new ResourceKey("RECORD", "DS1", "R001");
        assertEquals("RECORD:DS1:R001", key.toString());
    }

    @Test
    @Order(3000)
    void testToStringUrlEncodes() {
        // Characters that need URL encoding
        ResourceKey key = new ResourceKey("ENTITY", "test value", "foo:bar");
        String str = key.toString();
        // URL encoding replaces space with + or %20, colon with %3A
        assertTrue(str.contains("%") || str.contains("+"));
    }

    // ========================================================================
    // parse() Tests
    // ========================================================================

    @Test
    @Order(3100)
    void testParseNull() {
        assertNull(ResourceKey.parse(null));
    }

    @Test
    @Order(3200)
    void testParseEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> ResourceKey.parse(""));
    }

    @Test
    @Order(3300)
    void testParseWhitespaceOnly() {
        assertThrows(IllegalArgumentException.class, () -> ResourceKey.parse("   "));
    }

    @Test
    @Order(3400)
    void testParseSimpleKey() {
        ResourceKey key = ResourceKey.parse("ENTITY");
        assertEquals("ENTITY", key.getResourceType());
        assertTrue(key.getComponents().isEmpty());
    }

    @Test
    @Order(3500)
    void testParseKeyWithComponents() {
        ResourceKey key = ResourceKey.parse("ENTITY:123:456");
        assertEquals("ENTITY", key.getResourceType());
        assertEquals(List.of("123", "456"), key.getComponents());
    }

    @Test
    @Order(3600)
    void testParseRoundTrip() {
        ResourceKey original = new ResourceKey("RECORD", "DS1", "R001");
        String encoded = original.toString();
        ResourceKey parsed = ResourceKey.parse(encoded);
        assertEquals(original, parsed);
    }

    @Test
    @Order(3700)
    void testParseRoundTripWithSpecialCharacters() {
        ResourceKey original = new ResourceKey("ENTITY", "test value", "foo:bar");
        String encoded = original.toString();
        ResourceKey parsed = ResourceKey.parse(encoded);
        assertEquals(original, parsed);
    }

    @Test
    @Order(3800)
    void testParseWithLeadingWhitespace() {
        ResourceKey key = ResourceKey.parse("  ENTITY:123");
        assertEquals("ENTITY", key.getResourceType());
        assertEquals(List.of("123"), key.getComponents());
    }

    @Test
    @Order(3900)
    void testParseWithTrailingWhitespace() {
        ResourceKey key = ResourceKey.parse("ENTITY:123  ");
        assertEquals("ENTITY", key.getResourceType());
        assertEquals(List.of("123"), key.getComponents());
    }

    // ========================================================================
    // Additional compareTo() and equals() Tests
    // ========================================================================

    @Test
    @Order(4000)
    void testEqualsWhenResourceTypesDifferButComponentsSame() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("RECORD", "123");
        assertNotEquals(key1, key2);
    }

    @Test
    @Order(4100)
    void testEqualsWhenComponentsDifferButResourceTypeSame() {
        ResourceKey key1 = new ResourceKey("ENTITY", "123");
        ResourceKey key2 = new ResourceKey("ENTITY", "456");
        assertNotEquals(key1, key2);
    }

    @Test
    @Order(4200)
    void testCompareToWithMultipleComponentsFirstDiffers() {
        ResourceKey key1 = new ResourceKey("RECORD", "DS1", "R001");
        ResourceKey key2 = new ResourceKey("RECORD", "DS2", "R001");
        assertTrue(key1.compareTo(key2) < 0); // DS1 < DS2
    }

    @Test
    @Order(4300)
    void testCompareToWithMultipleComponentsLastDiffers() {
        ResourceKey key1 = new ResourceKey("RECORD", "DS1", "R001");
        ResourceKey key2 = new ResourceKey("RECORD", "DS1", "R002");
        assertTrue(key1.compareTo(key2) < 0); // R001 < R002
    }

    @Test
    @Order(4400)
    void testCompareToEmptyComponents() {
        ResourceKey key1 = new ResourceKey("ENTITY");
        ResourceKey key2 = new ResourceKey("ENTITY");
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    @Order(4500)
    void testToStringWithManySpecialCharacters() {
        ResourceKey key = new ResourceKey("TYPE", "a b c", "x:y:z", "p&q=r");
        String str = key.toString();
        // Verify round-trip works
        ResourceKey parsed = ResourceKey.parse(str);
        assertEquals(key, parsed);
    }

    @Test
    @Order(4600)
    void testCompareToWithEqualComponentsReturnsZero() {
        ResourceKey key1 = new ResourceKey("ENTITY", "AAA", "BBB");
        ResourceKey key2 = new ResourceKey("ENTITY", "AAA", "BBB");
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    @Order(4700)
    void testCompareToIteratesThroughAllComponents() {
        // Test that compareTo iterates through all matching components
        ResourceKey key1 = new ResourceKey("RECORD", "DS1", "R001", "EXTRA1");
        ResourceKey key2 = new ResourceKey("RECORD", "DS1", "R001", "EXTRA2");
        assertTrue(key1.compareTo(key2) < 0); // EXTRA1 < EXTRA2
    }
}
