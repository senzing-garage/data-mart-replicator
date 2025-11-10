package com.senzing.datamart;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.text.TextUtilities.*;

/**
 * Tests for {@link PostgreSqlUrl}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgreSqlUrlTest {

    public List<Arguments> getNullTestParameters() {
        List<Arguments> result = new LinkedList<>();
        result.add(Arguments.of(
            null, "password", "localhost", 6543, "MyDatabase"));
        result.add(Arguments.of(
            "joeSchmoe", null, "localhost", 6543, "MyDatabase"));
        result.add(Arguments.of(
            "joeSchmoe", "password", null, 6543, "MyDatabase"));
        result.add(Arguments.of(
            "joeSchmoe", "password", "localhost", 6543, null));
        return result;
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParameters(String   user,
                                   String   password,
                                   String   host,
                                   Integer  port,
                                   String   database)
    {
        try {
            new PostgreSqlUrl(user, password, host, database);

            fail("Unexpectedly succeeded in constructing with "
                + "a required null parameter.  user=[ " + user
                + " ], password=[ " + password + " ], host=[ " 
                + host + " ], database=[ " + database + " ]");

        } catch (NullPointerException expected) {
            // expected exception
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithPort(String   user,
                                           String   password,
                                           String   host,
                                           Integer  port,
                                           String   database)
    {
        try {
            new PostgreSqlUrl(user, password, host, port, database);

            fail("Unexpectedly succeeded in constructing with "
                + "a required null parameter.  user=[ " + user
                + " ], password=[ " + password + " ], host=[ " 
                + host + " ], port=[ " + port + " ], database=[ " 
                + database + " ]");

        } catch (NullPointerException expected) {
            // expected exception
        }
        
    }
    
    public List<Arguments> getConstructParameters() {
        List<Arguments> result = new LinkedList<>();
        for (int index = 0; index < 10; index++) {
            String user = randomPrintableText(8, 12);
            String password = randomPrintableText(12, 25);
            String host = randomPrintableText(10, 16);
            String database = randomPrintableText(8, 16);
            int port = 2000 + (index * 10);
            
            result.add(Arguments.of(
                user, password, host, port, database));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructor(String   user,
                                String   password,
                                String   host,
                                Integer  port,
                                String   database)
    {
        try {
            PostgreSqlUrl url = new PostgreSqlUrl(
                user, password, host, database);

            assertEquals(user, url.getUser(), "User is not as expected");
            assertEquals(password, url.getPassword(), "Password is not as expected");
            assertEquals(host, url.getHost(), "Host is not as expected");
            assertEquals(database, url.getDatabase(), "Database is not as expected");
            assertEquals(PostgreSqlUrl.DEFAULT_PORT, url.getPort(),
                "Port is not as expected");
            assertEquals(false, url.hasExplicitPort(), 
                "Explicit port flag is not as expected.");

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
                                        String  database)
    {
        try {
            PostgreSqlUrl url = new PostgreSqlUrl(
                user, password, host, port, database);

            assertEquals(user, url.getUser(), "User is not as expected");
            assertEquals(password, url.getPassword(), "Password is not as expected");
            assertEquals(host, url.getHost(), "Host is not as expected");
            assertEquals(database, url.getDatabase(), "Database is not as expected");
            assertEquals(port, url.getPort(), "Port is not as expected");
            assertEquals(true, url.hasExplicitPort(), 
                "Explicit port flag is not as expected.");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithUrl(String   user,
                                       String   password,
                                       String   host,
                                       Integer  port,
                                       String   database)
    {
        try {
            String urlText = PostgreSqlUrl.PREFIX + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) + ":" + port + "/"
                + urlEncodeUtf8(database);

            PostgreSqlUrl url = new PostgreSqlUrl(urlText);

            assertEquals(user, url.getUser(), "User is not as expected");
            assertEquals(password, url.getPassword(), "Password is not as expected");
            assertEquals(host, url.getHost(), "Host is not as expected");
            assertEquals(database, url.getDatabase(), "Database is not as expected");
            assertEquals(port, url.getPort(), "Port is not as expected");
            assertEquals(true, url.hasExplicitPort(), 
                "Explicit port flag is not as expected.");

            assertEquals(urlText, url.toString(),
                "Result from toString() not as expected.");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }        
    }


    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithUrlNoPort(String     user,
                                             String     password,
                                             String     host,
                                             Integer    port,
                                             String     database)
    {
        try {
            String urlText = PostgreSqlUrl.PREFIX + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) + "/" + urlEncodeUtf8(database);

            PostgreSqlUrl url = new PostgreSqlUrl(urlText);

            assertEquals(user, url.getUser(), "User is not as expected");
            assertEquals(password, url.getPassword(), "Password is not as expected");
            assertEquals(host, url.getHost(), "Host is not as expected");
            assertEquals(database, url.getDatabase(), "Database is not as expected");
            assertEquals(PostgreSqlUrl.DEFAULT_PORT, url.getPort(),
                "Port is not as expected");
            assertEquals(false, url.hasExplicitPort(), 
                "Explicit port flag is not as expected.");

            assertEquals(urlText, url.toString(),
                "Result from toString() not as expected.");

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
                          String    database)
    {
        try {
            String urlText = PostgreSqlUrl.PREFIX + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) + ":" + port + "/"
                + urlEncodeUtf8(database);

            PostgreSqlUrl url = PostgreSqlUrl.parse(urlText);

            assertEquals(user, url.getUser(), "User is not as expected");
            assertEquals(password, url.getPassword(), "Password is not as expected");
            assertEquals(host, url.getHost(), "Host is not as expected");
            assertEquals(database, url.getDatabase(), "Database is not as expected");
            assertEquals(port, url.getPort(), "Port is not as expected");
            assertEquals(true, url.hasExplicitPort(), 
                "Explicit port flag is not as expected.");

            assertEquals(urlText, url.toString(),
                "Result from toString() not as expected.");

        } catch (Exception e) {
            fail("Failed parsing with exception.", e);
        }        
    }


    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testParseNoPort(String  user,
                                String  password,
                                String  host,
                                Integer port,
                                String  database)
    {
        try {
            String urlText = PostgreSqlUrl.PREFIX + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) + "/" + urlEncodeUtf8(database);

            PostgreSqlUrl url = PostgreSqlUrl.parse(urlText);

            assertEquals(user, url.getUser(), "User is not as expected");
            assertEquals(password, url.getPassword(), "Password is not as expected");
            assertEquals(host, url.getHost(), "Host is not as expected");
            assertEquals(database, url.getDatabase(), "Database is not as expected");
            assertEquals(PostgreSqlUrl.DEFAULT_PORT, url.getPort(),
                "Port is not as expected");
            assertEquals(false, url.hasExplicitPort(), 
                "Explicit port flag is not as expected.");

            assertEquals(urlText, url.toString(),
                "Result from toString() not as expected.");

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
                                        String  database)
    {
        try {
            PostgreSqlUrl url1 = new PostgreSqlUrl(
                user, password, host, database);

            PostgreSqlUrl url2 = new PostgreSqlUrl(
                user, password, host, database);

            assertEquals(url1, url2, "Objects are unexpectedly not equal");
            assertEquals(url1.hashCode(), url2.hashCode(),
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
                                          String    database)
    {
        try {
            PostgreSqlUrl url1 = new PostgreSqlUrl(
                user, password, host, port, database);

            PostgreSqlUrl url2 = new PostgreSqlUrl(
                user, password, host, port, database);

            assertEquals(url1, url2, "Objects are unexpectedly not equal");
            assertEquals(url1.hashCode(), url2.hashCode(),
                "Objects unexpectedly have different hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }
        
    }

    public List<Arguments> getNotEqualsParameters() {
        List<Arguments> result = new LinkedList<>();
        for (int index = 0; index < 15; index++) {
            String user = randomPrintableText(8, 12);
            String password = randomPrintableText(12, 25);
            String host = randomPrintableText(10, 16);
            String database = randomPrintableText(8, 16);
            int port = 2000 + (index * 10);
            
            result.add(Arguments.of(
                user, password, host, port, database, index));
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
                                     int        changeIndex)
    {
        try {
            String user2        = user;
            String password2    = password;
            String host2        = host;
            int    port2        = port;
            String database2    = database;
            switch (changeIndex % 5) {
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
                    port2 = port + 5;
                    break;
                case 4:
                    database2 = database + "bax";
                    break;
                default:
                    throw new IllegalStateException(
                        "Bad case: " + (changeIndex % 5));
            }

            PostgreSqlUrl url1 = new PostgreSqlUrl(
                user, password, host, port, database);

            PostgreSqlUrl url2 = new PostgreSqlUrl(
                user2, password2, host2, port2, database2);

            assertNotEquals(url1, url2, "Objects are unexpectedly equal");
            assertNotEquals(url1.hashCode(), url2.hashCode(),
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
                                        String     database)
    {
        try {
            PostgreSqlUrl url1 = new PostgreSqlUrl(
                user, password, host, database);

            PostgreSqlUrl url2 = new PostgreSqlUrl(
                user, password, host, PostgreSqlUrl.DEFAULT_PORT, database);

            assertNotEquals(url1, url2, "Objects are unexpectedly equal");
            assertNotEquals(url1.hashCode(), url2.hashCode(),
                "Objects unexpectedly have same hash codes");

        } catch (Exception e) {
            fail("Failed construction with exception.", e);
        }


    }
}
