package com.senzing.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.senzing.text.TextUtilities;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.text.TextUtilities.*;
import static com.senzing.datamart.SqliteUri.*;

/**
 * Tests for {@link SqliteUri}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqliteUriTest {

    @Test
    public void testDefaultConstructor() {
        SqliteUri uri = new SqliteUri();

        assertEquals(true, uri.isMemory(), "uri.isMemory() not as expected");
        assertEquals(Collections.emptyMap(), uri.getQueryOptions(), "Query options are not empty");
        assertNull(uri.getFile(), "File is not null as expected");
        assertNull(uri.getUnusedUser(), "Unused user is not null");
        assertNull(uri.getUnusedPassword(), "Unused password is not null");
        assertNull(uri.getInMemoryIdentifier(), "Unused file is not null");
        assertEquals("", uri.getQueryString(), "Query string is not empty");

        assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(), "Scheme prefix is not as expected");
    }

    public List<Arguments> getConstructParameters() {
        try {
            List<Arguments> result = new LinkedList<>();

            result.add(Arguments.of(null, null, null, Map.of("foo", "bar")));

            result.add(Arguments.of(null, null, null, Map.of(MODE_KEY, MEMORY_MODE)));

            result.add(Arguments.of(null, null, null, Map.of(MODE_KEY, "rwc")));

            result.add(Arguments.of(null, null, File.createTempFile("test-", ".db"), Map.of("bar", "foo")));

            result.add(Arguments.of(null, null, File.createTempFile("test-", ".db"),
                    Map.of(MODE_KEY, MEMORY_MODE, "cache", "shared")));

            result.add(Arguments.of(null, null, null, null));

            result.add(Arguments.of(null, null, File.createTempFile("test-", ".db"), null));

            result.add(Arguments.of(null, null, File.createTempFile("test-", ".db"), null));

            result.add(Arguments.of("joe", "secret", File.createTempFile("test-", ".db"), Map.of("bar", "foo")));

            result.add(Arguments.of("jane", "mystery", File.createTempFile("test-", ".db"),
                    Map.of(MODE_KEY, MEMORY_MODE, "cache", "shared")));

            result.add(Arguments.of("jill", "unknown", File.createTempFile("test-", ".db"), null));

            result.add(Arguments.of("jack", null, File.createTempFile("test-", ".db"), null));

            result.add(Arguments.of(null, "hidden", File.createTempFile("test-", ".db"), null));

            result.add(Arguments.of(null, null, new File("C:\\temp\\test.db"), null));

            result.add(Arguments.of(null, null, new File("C:\\"), null));

            result.add(Arguments.of(null, null, new File("C:"), null));

            result.add(Arguments.of(null, null, new File("D:\\path with spaces\\file.db"), null));

            result.add(Arguments.of(null, null, new File("\\\\server\\share\\test.db"), null));

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testQueryConstructor(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            SqliteUri uri = new SqliteUri(queryOptions);

            if (queryOptions != null && queryOptions.containsKey(MODE_KEY)
                    && !MEMORY_MODE.equals(queryOptions.get(MODE_KEY))) {
                fail("Unexpectedly succeeded with non-memory-mode query options: " + queryOptions);
            }

            Map<String, String> expectedOptions = (queryOptions == null) ? new TreeMap<>() : queryOptions;

            if (queryOptions != null && MEMORY_MODE.equalsIgnoreCase(queryOptions.get(MODE_KEY))) {
                expectedOptions = new TreeMap<>(queryOptions);
                expectedOptions.remove(MODE_KEY);
            }

            assertEquals(true, uri.isMemory(), "uri.isMemory() not as expected");
            assertNull(uri.getFile(), "File is not null as expected");
            assertNull(uri.getUnusedUser(), "Unused user is not null");
            assertNull(uri.getUnusedPassword(), "Unused password is not null");
            assertNull(uri.getInMemoryIdentifier(), "Unused file is not null");

            assertEquals(expectedOptions, uri.getQueryOptions(), "Query options are not as expected");

            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(), "Scheme prefix is not as expected");

        } catch (IllegalArgumentException e) {
            if (queryOptions == null || MEMORY_MODE.equals(queryOptions.get(MODE_KEY))) {
                fail("Unexpectedly failed with null or memory-mode query options: " + queryOptions);
            }

        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testFileConstructor(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            SqliteUri uri = new SqliteUri(file);
            Map<String, String> expectedOptions = Collections.emptyMap();

            if (file == null) {
                fail("Unexpectedly succeeded with null file");
            }

            assertEquals(false, uri.isMemory(), "uri.isMemory() not as expected");
            assertEquals(file, uri.getFile(), "File is not as expected");
            assertNull(uri.getUnusedUser(), "Unused user is not null");
            assertNull(uri.getUnusedPassword(), "Unused password is not null");
            assertNull(uri.getInMemoryIdentifier(), "Unused file is not null");

            assertEquals(expectedOptions, uri.getQueryOptions(), "Query options are not null");
            assertEquals("", uri.getQueryString(), "Query string is not empty");

            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(), "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (file != null) {
                fail("Unexpectedly failed with a non-null file", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testFileQueryConstructor(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            SqliteUri uri = new SqliteUri(file, queryOptions);

            if (file == null) {
                fail("Unexpectedly succeeded with null file");
            }

            Map<String, String> expectedOptions = (queryOptions == null) ? new TreeMap<>() : queryOptions;

            if (queryOptions != null && MEMORY_MODE.equalsIgnoreCase(queryOptions.get(MODE_KEY))) {
                assertEquals(true, uri.isMemory(), "uri.isMemory() not as expected");
                assertNull(uri.getFile(), "File is not null null");
                assertEquals((file == null ? null : file.toString()),
                             uri.getInMemoryIdentifier(), "Unused file is not as expected");
            } else {
                assertEquals(false, uri.isMemory(), "uri.isMemory() not as expected");
                assertNull(uri.getInMemoryIdentifier(), "Unused file is not null null");
                assertEquals(file, uri.getFile(), "File is not as expected");
            }

            assertNull(uri.getUnusedUser(), "Unused user is not null");
            assertNull(uri.getUnusedPassword(), "Unused password is not null");

            assertEquals(expectedOptions, uri.getQueryOptions(), "Query options are not as expected");

            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(), "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (file != null) {
                fail("Unexpectedly failed with a non-null file", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testProtectedConstructor(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            SqliteUri uri = new SqliteUri(unusedUser, unusedPassword, file, queryOptions);

            if (file == null) {
                fail("Unexpectedly succeeded with null file");
            }
            if ((unusedUser == null && unusedPassword != null) || (unusedUser != null && unusedPassword == null)) {
                fail("Unexpectedly succeeded with incomplete user credentials.  " + "unusedUser=[ " + unusedUser
                        + " ], unusedPassword=[ " + unusedPassword + " ]");
            }

            Map<String, String> expectedOptions = (queryOptions == null) ? new TreeMap<>() : queryOptions;

            if (queryOptions != null && MEMORY_MODE.equalsIgnoreCase(queryOptions.get(MODE_KEY))) {
                assertEquals(true, uri.isMemory(), "uri.isMemory() not as expected");
                assertNull(uri.getFile(), "File is not null null");
                assertEquals((file == null) ? null : file.toString(), 
                              uri.getInMemoryIdentifier(), "Unused file is not as expected");
            } else {
                assertEquals(false, uri.isMemory(), "uri.isMemory() not as expected");
                assertNull(uri.getInMemoryIdentifier(), "Unused file is not null null");
                assertEquals(file, uri.getFile(), "File is not as expected");
            }

            assertEquals(unusedUser, uri.getUnusedUser(), "Unused user is not as expected");
            assertEquals(unusedPassword, uri.getUnusedPassword(), "Unused password is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(), "Query options are not as expected");

            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(), "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (file != null) {
                fail("Unexpectedly failed with a non-null file", e);
            }
        } catch (IllegalArgumentException e) {
            if ((unusedUser == null && unusedPassword == null) || (unusedUser != null && unusedPassword != null)) {
                fail("Unexpectedly failed with consistent user credentials.  " + "unusedUser=[ " + unusedUser
                        + " ], unusedPassword=[ " + unusedPassword + " ]");
            }

        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testParse(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        String uriText = null;
        try {
            // build the authority
            String authority = "";
            String expectedUser = null;
            String expectedPassword = null;

            if (unusedUser != null || unusedPassword != null && file != null) {
                expectedUser = (unusedUser == null) ? "na" : unusedUser;
                expectedPassword = (unusedPassword == null) ? "na" : unusedPassword;
                authority = expectedUser + ":" + expectedPassword + "@";
            }

            // normalize the query options
            queryOptions = (queryOptions != null) ? new TreeMap<>(queryOptions) : null;
            if (file == null && queryOptions != null && queryOptions.containsKey(MODE_KEY)
                    && !MEMORY_MODE.equals(queryOptions.get(MODE_KEY))) {
                queryOptions.put(MODE_KEY, MEMORY_MODE);
            }

            Map<String, String> expectedOptions = (queryOptions == null) ? new TreeMap<>() : queryOptions;

            if (file == null && queryOptions != null && MEMORY_MODE.equalsIgnoreCase(queryOptions.get(MODE_KEY))) {
                expectedOptions = new TreeMap<>(queryOptions);
                expectedOptions.remove(MODE_KEY);
            }

            // build the query string
            String queryString = "";
            if (queryOptions != null && queryOptions.size() > 0) {
                StringBuilder sb = new StringBuilder("?");
                queryOptions.forEach((key, value) -> {
                    sb.append(sb.length() == 1 ? "" : "&").append(urlEncodeUtf8(key)).append("=")
                            .append(urlEncodeUtf8(value));
                });
                queryString = sb.toString();
            }

            // build the expected query string
            String expectedQuery = "";
            if (expectedOptions != null && expectedOptions.size() > 0) {
                StringBuilder sb = new StringBuilder("?");
                expectedOptions.forEach((key, value) -> {
                    sb.append(sb.length() == 1 ? "" : "&").append(urlEncodeUtf8(key)).append("=")
                            .append(urlEncodeUtf8(value));
                });
                expectedQuery = sb.toString();
            }

            uriText = SqliteUri.SCHEME_PREFIX + ((file == null) ? ":memory:" : ("//" + authority + file)) + queryString;

            String expectedUri = SqliteUri.SCHEME_PREFIX + ((file == null) ? ":memory:" : ("//" + authority + file))
                    + expectedQuery;

            SqliteUri uri = SqliteUri.parse(uriText);

            assertEquals(expectedUser, uri.getUnusedUser(), "User is not as expected: " + uriText);
            assertEquals(expectedPassword, uri.getUnusedPassword(), "Password is not as expected: " + uriText);

            boolean expectedMemory = (file == null
                    || (queryOptions != null && MEMORY_MODE.equals(queryOptions.get(MODE_KEY))));
            assertEquals(expectedMemory, uri.isMemory(), "URI memory flag is not as expected: " + uriText);
            if (expectedMemory) {
                assertEquals((file == null ? null : file.toString()), 
                              uri.getInMemoryIdentifier(), "Unused file is not as expected: " + uriText);
                assertNull(uri.getFile(), "File is not null: " + uriText);
            } else {
                assertEquals(file, uri.getFile(), "File is not as expected: " + uriText);
                assertNull(uri.getInMemoryIdentifier(), "Unused file is not null: " + uriText);
            }
            assertEquals(expectedOptions, uri.getQueryOptions(), "Query options are not as expected: " + uriText);
            assertEquals(expectedQuery, uri.getQueryString(), "Query string is not as expected: " + uriText);
            assertTrue(
                    expectedUri.equals(uri.toString()) || expectedUri.equals(uri.toString().replaceAll("%20", " "))
                            || expectedUri.equals(uri.toString().replaceAll("\\+", " ")),
                    "Result from toString() (" + uri.toString() + ") not as expected: " + expectedUri);

            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(), "Scheme prefix is not as expected");

            ConnectionUri baseUri = ConnectionUri.parse(uriText);
            assertInstanceOf(uri.getClass(), baseUri, "Unexpected base URI parse: " + uriText);
            assertEquals(uri, baseUri, "Unexpected base URI parse: " + uriText);

        } catch (Exception e) {
            fail("Failed parsing with exception: " + uriText, e);
        }
    }

    @Test
    public void testEqualsAndHashDefault() {
        try {
            SqliteUri uri1 = new SqliteUri();

            SqliteUri uri2 = new SqliteUri();

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have different hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }

    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithQuery(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            // handle invalid arguments
            if (queryOptions != null && queryOptions.containsKey(MODE_KEY)
                    && !MEMORY_MODE.equals(queryOptions.get(MODE_KEY))) {
                queryOptions = new TreeMap<>(queryOptions);
                queryOptions.put(MODE_KEY, MEMORY_MODE);
            }

            SqliteUri uri1 = new SqliteUri(queryOptions);
            SqliteUri uri2 = new SqliteUri(queryOptions);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have different hash codes");

            Map<String, String> queryOptions2 = (queryOptions == null) ? new TreeMap<>() : new TreeMap<>(queryOptions);
            queryOptions2.put(TextUtilities.randomAlphabeticText(5), TextUtilities.randomAlphanumericText(5));

            uri1 = new SqliteUri(queryOptions);
            uri2 = new SqliteUri(queryOptions2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have the same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }

    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithFile(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            if (file == null) {
                file = File.createTempFile("test-", ".db");
            }
            SqliteUri uri1 = new SqliteUri(file);
            SqliteUri uri2 = new SqliteUri(file);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have different hash codes");

            File file2 = File.createTempFile("test-", ".db");

            uri1 = new SqliteUri(file);
            uri2 = new SqliteUri(file2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have the same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithFileAndQuery(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            if (file == null) {
                file = File.createTempFile("test-", ".db");
            }

            SqliteUri uri1 = new SqliteUri(file, queryOptions);
            SqliteUri uri2 = new SqliteUri(file, queryOptions);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have different hash codes");

            File file2 = file;
            Map<String, String> queryOptions2 = queryOptions;

            int changeIndex = Math.abs(file.toString().hashCode() % 2);
            switch (changeIndex) {
            case 0:
                file2 = File.createTempFile("test-", ".db");
                break;
            case 1:
                queryOptions2 = (queryOptions == null) ? new TreeMap<>() : new TreeMap<>(queryOptions);

                queryOptions2.put(TextUtilities.randomAlphabeticText(5), TextUtilities.randomAlphanumericText(5));
                break;
            default:
                fail("Unrecognized change index: " + changeIndex);
            }

            uri1 = new SqliteUri(file, queryOptions);
            uri2 = new SqliteUri(file2, queryOptions2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have the same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashProtected(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        try {
            if (file == null) {
                file = File.createTempFile("test-", ".db");
            }
            if (unusedUser != null || unusedPassword != null) {
                unusedUser = (unusedUser == null) ? "na" : unusedUser;
                unusedPassword = (unusedPassword == null) ? "na" : unusedPassword;
            }

            SqliteUri uri1 = new SqliteUri(unusedUser, unusedPassword, file, queryOptions);
            SqliteUri uri2 = new SqliteUri(unusedUser, unusedPassword, file, queryOptions);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have different hash codes");

            String unusedUser2 = unusedUser;
            String unusedPassword2 = unusedPassword;
            File file2 = file;
            Map<String, String> queryOptions2 = queryOptions;

            int changeIndex = Math.abs(file.toString().hashCode() % 4);
            switch (changeIndex) {
            case 0:
                file2 = File.createTempFile("test-", ".db");
                break;

            case 1:
                queryOptions2 = (queryOptions == null) ? new TreeMap<>() : new TreeMap<>(queryOptions);

                queryOptions2.put(TextUtilities.randomAlphabeticText(5), TextUtilities.randomAlphanumericText(5));
                break;

            case 2:
                unusedUser2 = (unusedUser == null) ? "na" : unusedUser + "Smith";
                unusedPassword2 = (unusedPassword2 == null) ? "na" : unusedPassword2;
                break;

            case 3:
                unusedPassword2 = (unusedPassword == null) ? "na" : unusedPassword + "XXX";
                unusedUser2 = (unusedUser2 == null) ? "na" : unusedUser2;
                break;

            default:
                fail("Unrecognized change index: " + changeIndex);
            }

            uri1 = new SqliteUri(unusedUser, unusedPassword, file, queryOptions);
            uri2 = new SqliteUri(unusedUser2, unusedPassword2, file2, queryOptions2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(), "Objects unexpectedly have the same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
    }

    @ParameterizedTest
    @CsvSource({ "sqlite3://*:", "sqlite2:///tmp/test.db", "sqlite3://foo\\bar\\phoo", "sqlite3://foo/bar  /ph%o" })
    public void testBadUriParse(String text) {
        try {
            SqliteUri.parse(text);

            fail("Succeeded in parsing illegal SQLite URI: " + text);
        } catch (IllegalArgumentException expected) {
            // all is well
        } catch (Exception e) {
            fail("Got unexpected exception parsing illegal SQLite URI: " + text, e);
        }
    }

}
