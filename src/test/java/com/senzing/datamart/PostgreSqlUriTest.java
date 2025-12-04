package com.senzing.datamart;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.text.TextUtilities.*;
import static com.senzing.datamart.PostgreSqlUri.*;

/**
 * Tests for {@link PostgreSqlUri}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgreSqlUriTest {

    public List<Arguments> getNullTestParameters() {
        List<Arguments> result = new LinkedList<>();
        result.add(Arguments.of(
            null, "password", "localhost", 6543, "MyDatabase", "MySchema"));
        result.add(Arguments.of(
            "joeSchmoe", null, "localhost", 6543, "MyDatabase", "MySchema"));
        result.add(Arguments.of(
            "joeSchmoe", "password", null, 7821, "MyDatabase", "MySchema"));
        result.add(Arguments.of(
            "joeSchmoe", "password", "localhost", null, "MyDatabase", "MySchema"));
        result.add(Arguments.of(
            "joeSchmoe", "password", "localhost", 9567, null, "MySchema"));
        result.add(Arguments.of(
            "joeSchmoe", "password", "localhost", 3451, "MyDatabase", null));
        return result;
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParameters(String   user,
                                   String   password,
                                   String   host,
                                   Integer  port,
                                   String   database,
                                   String   schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(user, password, host, database);

            if (user == null || password == null || host == null || database == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], database=[ " + database + " ]");
            }

            Map<String, String> expectedOptions = Collections.emptyMap();

            assertEquals(DEFAULT_PORT, uri.getPort(),
                         "Port was not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(),
                         "Database is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");
            assertEquals(DEFAULT_SCHEMA, uri.getSchema(), "Schema is not as expected");
            assertEquals(false, uri.hasSchema(), "Schema flag is not as expected");
            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null && database != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], database=[ " + database + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithSchema(String     user,
                                             String     password,
                                             String     host,
                                             Integer    port,
                                             String     database,
                                             String     schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(user, password, host, database, schema);

            if (user == null || password == null || host == null || database == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], database=[ " + database 
                    + " ], schema=[ " + schema + " ]");
            }

            Map<String, String> expectedOptions = (schema == null) 
                ? Collections.emptyMap() : Map.of(SCHEMA_KEY, schema);
            
            String expectedSchema = (schema == null) ? DEFAULT_SCHEMA : schema;

            assertEquals(DEFAULT_PORT, uri.getPort(),
                         "Port was not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(),
                         "Database is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");
            assertEquals(expectedSchema, uri.getSchema(), "Schema is not as expected");
            assertEquals((schema != null), uri.hasSchema(), 
                          "Schema flag is not as expected");

            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");
            
        } catch (NullPointerException e) {
            if (user != null && password != null && host != null && database != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " 
                    + user + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], database=[ " + database + " ], schema=[ "
                    + schema + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithPort(String   user,
                                           String   password,
                                           String   host,
                                           Integer  port,
                                           String   database,
                                           String   schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(user, password, host, port, database);

            if (user == null || password == null || host == null || database == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], port=[ " + port + " ], database=[ " 
                    + database + " ]");
            }

            Map<String, String> expectedOptions = Collections.emptyMap();
            int expectedPort = (port == null) ? DEFAULT_PORT : port;

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(),
                         "Database is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");
            assertEquals(DEFAULT_SCHEMA, uri.getSchema(), "Schema is not as expected");
            assertEquals(false, uri.hasSchema(), "Schema flag is not as expected");
            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null && database != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], port=[ " + port + " ], database=[ " 
                    + database + " ]", e);
            }
        }        
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithSchemaAndPort(String   user,
                                                    String   password,
                                                    String   host,
                                                    Integer  port,
                                                    String   database,
                                                    String   schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(
                user, password, host, port, database, schema);

            if (user == null || password == null || host == null || database == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], port=[ " + port + " ], database=[ " 
                    + database + " ], schema=[ " + schema + " ]");
            }

            Map<String, String> expectedOptions = (schema == null) 
                ? Collections.emptyMap() : Map.of(SCHEMA_KEY, schema);
            
            int expectedPort = (port == null) ? DEFAULT_PORT : port;

            String expectedSchema = (schema == null) ? DEFAULT_SCHEMA : schema;

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(),
                         "Database is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");
            assertEquals(expectedSchema, uri.getSchema(), "Schema is not as expected");
            assertEquals((schema != null), uri.hasSchema(), 
                          "Schema flag is not as expected");
            assertEquals(SCHEME_PREFIX, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null && database != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ], port=[ " + port + " ], database=[ " 
                    + database + " ], schema=[ " + schema + " ]", e);
            }
        }        
    }
    
    public List<Arguments> getConstructParameters() {
        List<Arguments> result = new LinkedList<>();
        result.add(Arguments.of(
            "joe", "secret", "localhost", 1234, "MyDatabase", "MySchema"));

        result.add(Arguments.of(
            "john", "hidden", "server1", null, "SomeDatabase", "SomeSchema"));

        result.add(Arguments.of(
            "jane", "unknown", "server2", 43210, "ThatDatabase", null));

        result.add(Arguments.of(
            "jeff", "mystery", "server3", null, "ThisDatabase", null));

        result.add(Arguments.of(
            "jeff", "surprise", "server4", DEFAULT_PORT, "ThisDatabase", null));

        result.add(Arguments.of(
            "jill", "complex", "server5", null, "ADatabase", DEFAULT_SCHEMA));

        result.add(Arguments.of(
            "jack", "password", "server6", DEFAULT_PORT, "TheDatabase", DEFAULT_SCHEMA));

        for (int index = 0; index < 10; index++) {
            String user = randomPrintableText(8, 12);
            String password = randomPrintableText(12, 25);
            String host = randomPrintableText(10, 16);
            String database = randomPrintableText(8, 16);
            String schema = randomPrintableText(5,12);
            int port = 2000 + (index * 10);
            
            result.add(Arguments.of(
                user, password, host, port, database, schema));
            result.add(Arguments.of(
                user, password, host, null, database, schema));
            result.add(Arguments.of(
                user, password, host, port, database, null));
            result.add(Arguments.of(
                user, password, host, null, database, null));
            result.add(Arguments.of(
                user, password, host, DEFAULT_PORT, database, null));
            result.add(Arguments.of(
                user, password, host, null, database, DEFAULT_SCHEMA));
            result.add(Arguments.of(
                user, password, host, DEFAULT_PORT, database, DEFAULT_SCHEMA));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructor(String   user,
                                String   password,
                                String   host,
                                Integer  port,
                                String   database,
                                String   schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(
                user, password, host, database);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(), "Database is not as expected");
            assertEquals(DEFAULT_PORT, uri.getPort(),
                "Port is not as expected");
            assertEquals(false, uri.hasPort(), 
                "Explicit port flag is not as expected.");
            assertEquals(false, uri.hasSchema(), 
                "Explicit schema flag is not as expected.");
            assertEquals(DEFAULT_SCHEMA, uri.getSchema(),
                         "Schema is not as expected");
            assertEquals(0, uri.getQueryOptions().size(), 
                        "Query options are not empty as expected");
            assertEquals("", uri.getQueryString(), 
                         "Query string is not empty");
            assertEquals(false, uri.hasSchema(), 
                        "Explicit schema flag is not as expected.");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithPort(String  user,
                                        String  password,
                                        String  host,
                                        Integer port,
                                        String  database,
                                        String  schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(
                user, password, host, port, database);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(), "Database is not as expected");
            if (port == null) {
                assertEquals(DEFAULT_PORT, uri.getPort(), "Port is not as expected");
                assertEquals(false, uri.hasPort(), 
                    "Explicit port flag is not as expected.");

            } else {
                assertEquals(port, uri.getPort(), "Port is not as expected");
                assertEquals(true, uri.hasPort(), 
                    "Explicit port flag is not as expected.");
            }
            assertEquals(DEFAULT_SCHEMA, uri.getSchema(),
                         "Schema is not as expected");
            assertEquals(0, uri.getQueryOptions().size(), 
                        "Query options are not empty as expected");
            assertEquals("", uri.getQueryString(), 
                         "Query string is not empty");
            assertEquals(false, uri.hasSchema(), 
                        "Explicit schema flag is not as expected.");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithSchema(String    user,
                                          String    password,
                                          String    host,
                                          Integer   port,
                                          String    database,
                                          String    schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(
                user, password, host, database, schema);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(), "Database is not as expected");
            assertEquals(DEFAULT_PORT, uri.getPort(),
                "Port is not as expected");
            assertEquals(false, uri.hasPort(), 
                "Explicit port flag is not as expected.");
            if (schema == null) {
                assertEquals(DEFAULT_SCHEMA, uri.getSchema(),
                             "Schema is not as expected");
                assertEquals(0, uri.getQueryOptions().size(), 
                             "Query options are not empty as expected");
                assertEquals("", uri.getQueryString(), 
                             "Query string is not empty");
                assertEquals(false, uri.hasSchema(), 
                             "Explicit schema flag is not as expected.");
            } else {
                assertEquals(schema, uri.getSchema(), "Schema is not as expected");
                assertEquals(1, uri.getQueryOptions().size(), 
                            "Query option count is not as expected.");
                assertEquals(schema, uri.getQueryOptions().get(SCHEMA_KEY), 
                            "Schema query option is not as expected.");
                assertEquals(true, uri.hasSchema(), 
                    "Explicit schema flag is not as expected.");
                assertEquals("?" + SCHEMA_KEY + "=" + urlEncodeUtf8(schema), 
                             uri.getQueryString(), 
                             "Query string is not as expected");
            }

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }


    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithPortAndSchema(String     user,
                                                 String     password,
                                                 String     host,
                                                 Integer    port,
                                                 String     database,
                                                 String     schema)
    {
        try {
            PostgreSqlUri uri = new PostgreSqlUri(
                user, password, host, port, database, schema);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(), "Database is not as expected");
            if (port == null) {
                assertEquals(DEFAULT_PORT, uri.getPort(), "Port is not as expected");
                assertEquals(false, uri.hasPort(), 
                    "Explicit port flag is not as expected.");

            } else {
                assertEquals(port, uri.getPort(), "Port is not as expected");
                assertEquals(true, uri.hasPort(), 
                    "Explicit port flag is not as expected.");
            }
            if (schema == null) {
                assertEquals(DEFAULT_SCHEMA, uri.getSchema(),
                             "Schema is not as expected");
                assertEquals(0, uri.getQueryOptions().size(), 
                             "Query options are not empty as expected");
                assertEquals("", uri.getQueryString(), 
                             "Query string is not empty");
                assertEquals(false, uri.hasSchema(), 
                             "Explicit schema flag is not as expected.");
            } else {
                assertEquals(schema, uri.getSchema(), "Schema is not as expected");
                assertEquals(1, uri.getQueryOptions().size(), 
                            "Query option count is not as expected.");
                assertEquals(schema, uri.getQueryOptions().get(SCHEMA_KEY), 
                            "Schema query option is not as expected.");
                assertEquals(true, uri.hasSchema(), 
                    "Explicit schema flag is not as expected.");
                assertEquals("?" + SCHEMA_KEY + "=" + urlEncodeUtf8(schema), 
                             uri.getQueryString(), 
                             "Query string is not as expected");
            }

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testParse(String    user,
                          String    password,
                          String    host,
                          Integer   port,
                          String    database,
                          String    schema)
    {
        try {
            String uriText = PostgreSqlUri.SCHEME_PREFIX 
                + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) 
                + ((port == null) ? "" : (":" + port))
                + "/" + urlEncodeUtf8(database)
                + ((schema == null) ? "" 
                    : "?" + SCHEMA_KEY + "=" + urlEncodeUtf8(schema));

            PostgreSqlUri uri = PostgreSqlUri.parse(uriText);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(), "Database is not as expected");
            if (port == null) {
                assertEquals(DEFAULT_PORT, uri.getPort(), "Port is not as expected");
                assertEquals(false, uri.hasPort(), 
                    "Explicit port flag is not as expected.");
            } else {
                assertEquals(port, uri.getPort(), "Port is not as expected");
                assertEquals(true, uri.hasPort(), 
                    "Explicit port flag is not as expected.");
            }

            if (schema == null) {
                assertEquals(DEFAULT_SCHEMA, uri.getSchema(),
                             "Schema is not as expected");
                assertEquals(0, uri.getQueryOptions().size(), 
                             "Query options are not empty as expected");
                assertEquals("", uri.getQueryString(), 
                             "Query string is not empty");
                assertEquals(false, uri.hasSchema(), 
                             "Explicit schema flag is not as expected.");
            } else {
                assertEquals(schema, uri.getSchema(), "Schema is not as expected");
                assertEquals(1, uri.getQueryOptions().size(), 
                            "Query option count is not as expected.");
                assertEquals(schema, uri.getQueryOptions().get(SCHEMA_KEY), 
                            "Schema query option is not as expected.");
                assertEquals(true, uri.hasSchema(), 
                             "Explicit schema flag is not as expected.");
                assertEquals("?" + SCHEMA_KEY + "=" + urlEncodeUtf8(schema), 
                             uri.getQueryString(), 
                             "Query string is not as expected");
            }

            assertEquals(uriText, uri.toString(),
                "Result from toString() not as expected.");

            ConnectionUri baseUri = ConnectionUri.parse(uriText);
            assertInstanceOf(uri.getClass(), baseUri,
                             "Unexpected base URI parse: " + uriText);
            assertEquals(uri, baseUri, 
                         "Unexpected base URI parse: " + uriText);
            
            String altUriText = PostgreSqlUri.SCHEME_PREFIX 
                + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) 
                + ((port == null) ? "" : (":" + port))
                + ":" + urlEncodeUtf8(database) + "/"
                + ((schema == null) ? "" 
                    : "?" + SCHEMA_KEY + "=" + urlEncodeUtf8(schema));

            uri = PostgreSqlUri.parse(altUriText);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(database, uri.getDatabase(), "Database is not as expected");
            if (port == null) {
                assertEquals(DEFAULT_PORT, uri.getPort(), "Port is not as expected");
                assertEquals(false, uri.hasPort(), 
                    "Explicit port flag is not as expected.");
            } else {
                assertEquals(port, uri.getPort(), "Port is not as expected");
                assertEquals(true, uri.hasPort(), 
                    "Explicit port flag is not as expected.");
            }
            if (schema == null) {
                assertEquals(DEFAULT_SCHEMA, uri.getSchema(),
                             "Schema is not as expected");
                assertEquals(0, uri.getQueryOptions().size(), 
                            "Query options are not empty as expected");
                assertEquals("", uri.getQueryString(), 
                             "Query string is not empty");
                assertEquals(false, uri.hasSchema(), 
                    "Explicit schema flag is not as expected.");
            } else {
                assertEquals(schema, uri.getSchema(), "Schema is not as expected");
                assertEquals(1, uri.getQueryOptions().size(), 
                            "Query option count is not as expected.");
                assertEquals(schema, uri.getQueryOptions().get(SCHEMA_KEY), 
                            "Schema query option is not as expected.");
                assertEquals("?" + SCHEMA_KEY + "=" + urlEncodeUtf8(schema), 
                             uri.getQueryString(), 
                             "Query string is not as expected");
                assertEquals(true, uri.hasSchema(), 
                    "Explicit schema flag is not as expected.");
            }

            assertEquals(uriText, uri.toString(),
                "Result from toString() not as expected.");

            baseUri = ConnectionUri.parse(altUriText);
            assertInstanceOf(uri.getClass(), baseUri,
                             "Unexpected base URI parse: " + uriText);
            assertEquals(uri, baseUri, 
                         "Unexpected base URI parse: " + uriText);


        } catch (Exception e) {
            fail("Failed parsing with exception.", e);
        }        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashNoPort(String  user,
                                        String  password,
                                        String  host,
                                        Integer port,
                                        String  database,
                                        String  schema)
    {
        try {
            PostgreSqlUri uri1 = new PostgreSqlUri(
                user, password, host, database);

            PostgreSqlUri uri2 = new PostgreSqlUri(
                user, password, host, database);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithPort(String    user,
                                          String    password,
                                          String    host,
                                          Integer   port,
                                          String    database,
                                          String    schema)
    {
        try {
            PostgreSqlUri uri1 = new PostgreSqlUri(
                user, password, host, port, database);

            PostgreSqlUri uri2 = new PostgreSqlUri(
                user, password, host, port, database);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    public List<Arguments> getNotEqualsParameters() {
        List<Arguments> result = new LinkedList<>();
        int changeIndex = 0;
        for (int index = 0; index < 50; index++) {
            String user = randomPrintableText(8, 12);
            String password = randomPrintableText(12, 25);
            String host = randomPrintableText(10, 16);
            String database = randomPrintableText(8, 16);
            String schema = randomPrintableText(5, 12);
            int port = 2000 + (index * 10);
            
            result.add(Arguments.of(
                user, password, host, port, database, schema, changeIndex++));
            result.add(Arguments.of(
                user, password, host, null, database, schema, changeIndex++));
            result.add(Arguments.of(
                user, password, host, port, database, null, changeIndex++));
            result.add(Arguments.of(
                user, password, host, null, database, null, changeIndex++));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("getNotEqualsParameters")
    public void testNotEqualsAndHash(String     user,
                                     String     password,
                                     String     host,
                                     Integer    port,
                                     String     database,
                                     String     schema,
                                     int        changeIndex)
    {
        try {
            String  user2       = user;
            String  password2   = password;
            String  host2       = host;
            Integer port2       = port;
            String  database2   = database;
            String  schema2     = schema;
            switch (changeIndex % 6) {
                case 0:
                    user2 = user + "foo";
                    break;
                case 1:
                    password2 = password + "bar";
                    break;
                case 2:
                    host2 = host + "phoo";
                    break;
                case 3:
                    port2 = (port == null) ? 2134 : (port + 5);
                    break;
                case 4:
                    database2 = database + "bax";
                    break;
                case 5:
                    schema2 = (schema == null) ? "not-null" : (schema + "foobar");
                    break;
                default:
                    throw new IllegalStateException(
                        "Bad case: " + (changeIndex % 5));
            }

            PostgreSqlUri uri1 = new PostgreSqlUri(
                user, password, host, port, database, schema);

            PostgreSqlUri uri2 = new PostgreSqlUri(
                user2, password2, host2, port2, database2, schema2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have same hash codes");

        } catch (Exception e) {
            fail("Failed with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testDefaultPortNotEqual(String     user,
                                        String     password,
                                        String     host,
                                        Integer    port,
                                        String     database,
                                        String     schema)
    {
        try {
            PostgreSqlUri uri1 = new PostgreSqlUri(
                user, password, host, database);

            PostgreSqlUri uri2 = new PostgreSqlUri(
                user, password, host, DEFAULT_PORT, database);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testDefaultSchemaNotEqual(String    user,
                                          String    password,
                                          String    host,
                                          Integer   port,
                                          String    database,
                                          String    schema)
    {
        try {
            PostgreSqlUri uri1 = new PostgreSqlUri(
                user, password, host, database);

            PostgreSqlUri uri2 = new PostgreSqlUri(
                user, password, host, database, DEFAULT_SCHEMA);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testDefaultSchemaWithPortNotEqual(String    user,
                                                  String    password,
                                                  String    host,
                                                  Integer   port,
                                                  String    database,
                                                  String    schema)
    {
        try {
            PostgreSqlUri uri1 = new PostgreSqlUri(
                user, password, host, port, database);

            PostgreSqlUri uri2 = new PostgreSqlUri(
                user, password, host, port, database, DEFAULT_SCHEMA);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }


    }
}
