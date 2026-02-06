package com.senzing.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BuildInfo}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BuildInfoTest {

    /**
     * Test that MAVEN_VERSION is initialized and not null.
     */
    @Test
    public void testMavenVersionNotNull() {
        assertNotNull(BuildInfo.MAVEN_VERSION,
            "MAVEN_VERSION should be initialized (not null)");
    }

    /**
     * Test that MAVEN_VERSION is not empty.
     */
    @Test
    public void testMavenVersionNotEmpty() {
        assertNotNull(BuildInfo.MAVEN_VERSION);
        assertFalse(BuildInfo.MAVEN_VERSION.trim().isEmpty(),
            "MAVEN_VERSION should not be empty");
    }

    /**
     * Test that MAVEN_VERSION matches expected format or is UNKNOWN.
     * In test classpath, it might be UNKNOWN (file missing) or ${project.version} (unfiltered).
     * In packaged JAR, it should be a version like "2.0.0-beta.1.4".
     */
    @Test
    public void testMavenVersionFormat() {
        String version = BuildInfo.MAVEN_VERSION;

        assertNotNull(version);

        // Should be one of:
        // - "UNKNOWN" (resource file not found)
        // - "${project.version}" (Maven placeholder not filtered yet - test classpath)
        // - Semantic version pattern (x.y.z) (properly filtered - packaged JAR)
        assertTrue(version.equals("UNKNOWN") ||
                   version.equals("${project.version}") ||
                   version.matches("\\d+\\.\\d+\\.\\d+.*"),
            "MAVEN_VERSION should be 'UNKNOWN', '${project.version}', or match semantic version pattern (x.y.z), got: " + version);
    }

    /**
     * Test that build-info.properties resource is accessible.
     * This verifies the static initializer can load the resource.
     */
    @Test
    public void testBuildInfoResourceExists() throws Exception {
        String resource = "/com/senzing/datamart/build-info.properties";

        try (InputStream is = BuildInfo.class.getResourceAsStream(resource)) {
            if (is != null) {
                // Resource exists - verify it can be loaded
                Properties props = new Properties();
                props.load(is);

                // Verify it contains Maven-Version property
                assertTrue(props.containsKey("Maven-Version"),
                    "build-info.properties should contain Maven-Version property");

                String version = props.getProperty("Maven-Version");
                assertNotNull(version, "Maven-Version property should not be null");
                assertFalse(version.trim().isEmpty(),
                    "Maven-Version property should not be empty");

                // Verify it matches MAVEN_VERSION constant
                assertEquals(version, BuildInfo.MAVEN_VERSION,
                    "MAVEN_VERSION should match value from build-info.properties");

            } else {
                // Resource doesn't exist (test classpath scenario)
                // MAVEN_VERSION should be "UNKNOWN"
                assertEquals("UNKNOWN", BuildInfo.MAVEN_VERSION,
                    "MAVEN_VERSION should be 'UNKNOWN' when build-info.properties is not found");
            }
        }
    }

    /**
     * Test that MAVEN_VERSION is a valid semantic version when available.
     * This test is lenient - it passes if version is UNKNOWN or unfiltered placeholder.
     */
    @Test
    public void testMavenVersionIsValidWhenAvailable() {
        String version = BuildInfo.MAVEN_VERSION;

        // Skip validation for special cases
        if (version.equals("UNKNOWN") || version.startsWith("${")) {
            // Test passes - these are expected in test classpath
            return;
        }

        // If it's a real version, validate format
        String[] parts = version.split("\\.");
        assertTrue(parts.length >= 3,
            "Version should have at least major.minor.patch components: " + version);

        // First three parts should be numeric
        assertTrue(parts[0].matches("\\d+"),
            "Major version should be numeric: " + parts[0]);
        assertTrue(parts[1].matches("\\d+"),
            "Minor version should be numeric: " + parts[1]);
        assertTrue(parts[2].matches("\\d+.*"),
            "Patch version should start with number: " + parts[2]);
    }

    /**
     * Test that the BuildInfo class cannot be instantiated.
     * The constructor is private, so this is verified by reflection.
     */
    @Test
    public void testPrivateConstructor() throws Exception {
        java.lang.reflect.Constructor<BuildInfo> constructor =
            BuildInfo.class.getDeclaredConstructor();

        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()),
            "Constructor should be private");

        // Verify it can't be instantiated without making it accessible
        assertThrows(IllegalAccessException.class, () -> {
            constructor.newInstance();
        }, "Should not be able to instantiate without making accessible");
    }
}
