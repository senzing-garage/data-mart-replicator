package com.senzing.datamart;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.text.TextUtilities.*;
import static com.senzing.datamart.RabbitMqUri.*;

/**
 * Tests for {@link RabbitMqUri}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RabbitMqUriTest {

    public List<Arguments> getNullTestParameters() {
        List<Arguments> result = new LinkedList<>();
        boolean[] secureFlags = { true, false };
        for (boolean secure : secureFlags) {
            result.add(Arguments.of(
                secure, null, "password", "localhost", 6543, "MyVirtualHost", 
                Map.of("foo", "bar")));
            result.add(Arguments.of(
                secure, "joeSchmoe", null, "localhost", 6543, "MyVirtualHost", 
                Map.of("foo", "bar")));
            result.add(Arguments.of(
                secure, "joeSchmoe", "password", null, 7821, "MyVirtualHost",
                Map.of("foo", "bar")));
            result.add(Arguments.of(
                secure, "joeSchmoe", "password", "localhost", null, "MyVirtualHost",
                Map.of("foo", "bar")));
            result.add(Arguments.of(
                secure, "joeSchmoe", "password", "localhost", 9567, null,
                Map.of("foo", "bar")));
            result.add(Arguments.of(
                secure, "joeSchmoe", "password", "localhost", 3451, "MyVirtualHost",
                Map.of("foo", "bar")));
            result.add(Arguments.of(
                secure, "joeSchmoe", "password", "localhost", 3451, "MyVirtualHost",
                null));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParameters(boolean              secure,
                                   String               user,
                                   String               password,
                                   String               host,
                                   Integer              port,
                                   String               virtualHost,
                                   Map<String, String>  queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(secure, user, password, host);

            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = Collections.emptyMap();

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithVirtualHost(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, virtualHost);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = Collections.emptyMap();

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithPort(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = Collections.emptyMap();

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");


        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, queryOptions);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = (queryOptions == null) 
                ? Collections.emptyMap() : queryOptions;

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithPortAndVirtualHost(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port, virtualHost);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = Collections.emptyMap();

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");


        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithVirtualHostAndQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, virtualHost, queryOptions);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = (queryOptions == null) 
                ? Collections.emptyMap() : queryOptions;

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithPortAndQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port, queryOptions);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = (queryOptions == null) 
                ? Collections.emptyMap() : queryOptions;

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getNullTestParameters")
    public void testNullParametersWithAll(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port, virtualHost, queryOptions);
                
            if (user == null || password == null || host == null) {
                fail("Unexpectedly succeeded in constructing with "
                    + "a required null parameter.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]");
            }

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = (queryOptions == null) 
                ? Collections.emptyMap() : queryOptions;

            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (NullPointerException e) {
            if (user != null && password != null && host != null) {
                fail("Unexpectedly failed in constructing with "
                    + "all required parameters not null.  user=[ " + user
                    + " ], password=[ " + password + " ], host=[ " 
                    + host + " ]", e);
            }
        }
    }
    
    public List<Arguments> getConstructParameters() {
        List<Arguments> result = new LinkedList<>();
        boolean[] secureFlags = { true, false };
        for (boolean secure : secureFlags) {
            result.add(Arguments.of(
                secure, "joe", "secret", "localhost", 1234, "MyVirtualHost",
                Map.of("foo", "bar")));

            result.add(Arguments.of(
                secure, "john", "hidden", "server1", null, "TheVirtualHost",
                Map.of("foo", "bar")));

            result.add(Arguments.of(
                secure, "jane", "unknown", "server2", 43210, "ThatVirtualHost",
                null));

            result.add(Arguments.of(
                secure, "jeff", "mystery", "server3", null, "ThisVirtualHost",
                null));

            result.add(Arguments.of(
                secure, "jeff", "surprise", "server4",
                (secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT),
                "ThisVirtualHost", null));

            result.add(Arguments.of(
                secure, "jill", "complex", "server5", null, "AVirtualHost",
                Collections.emptyMap()));

            result.add(Arguments.of(
                secure, "jack", "password", "server6", 
                (secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT),
                "TheVirtualHost", Collections.emptyMap()));

            for (int index = 0; index < 10; index++) {
                String user = randomPrintableText(8, 12);
                String password = randomPrintableText(12, 25);
                String host = randomPrintableText(10, 16);
                String virtualHost = randomPrintableText(8, 16);
                int port = 2000 + (index * 10);
                
                Map<String, String> queryOptions = Map.of(
                    randomPrintableText(3, 8), 
                    randomPrintableText(5, 8));
                
                result.add(Arguments.of(
                    secure, user, password, host, port, virtualHost, queryOptions));
                result.add(Arguments.of(
                    secure, user, password, host, null, virtualHost, queryOptions));
                result.add(Arguments.of(
                    secure, user, password, host, port, virtualHost, null));
                result.add(Arguments.of(
                    secure, user, password, host, null, virtualHost, null));
                result.add(Arguments.of(
                    secure, user, password, host,
                    (secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT), 
                    virtualHost, null));
                result.add(Arguments.of(
                    secure, user, password, host, null, virtualHost, 
                    Collections.emptyMap()));
                result.add(Arguments.of(
                    secure, user, password, host, 
                    (secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT),
                    virtualHost, Collections.emptyMap()));
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructor(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host);

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = Collections.emptyMap();
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithPort(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port);

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = Collections.emptyMap();
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithVirtualHost(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, virtualHost);

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = Collections.emptyMap();
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, queryOptions);

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithPortAndVirtualHost(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port, virtualHost);

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = Collections.emptyMap();
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithPortAndQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port, queryOptions);

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertNull(uri.getVirtualHost(), 
                       "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithVirtualHostAndQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host,virtualHost, queryOptions);

            int expectedPort = (secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
            
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals(false, uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructorWithAll(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri = new RabbitMqUri(
                secure, user, password, host, port, virtualHost, queryOptions);

            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testParse(boolean               secure,
                          String                user,
                          String                password,
                          String                host,
                          Integer               port,
                          String                virtualHost,
                          Map<String, String>   queryOptions)
    {
        try {
            String queryString = "";
            if (queryOptions != null && queryOptions.size() > 0) {
                StringBuilder sb = new StringBuilder("?");
                queryOptions.forEach((key, value) -> {
                    sb.append((sb.length() > 1) ? "&" : "")
                      .append(urlEncodeUtf8(key)).append("=")
                      .append(urlEncodeUtf8(value));
                });
                queryString = sb.toString();
            }

            String uriText 
                = (secure ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX)
                + urlEncodeUtf8(user)
                + ":" + urlEncodeUtf8(password) + "@" 
                + urlEncodeUtf8(host) 
                + ((port == null) ? "" : (":" + port))
                + "/" 
                + ((virtualHost == null) ? "" : urlEncodeUtf8(virtualHost))
                + queryString;
                
            RabbitMqUri uri = RabbitMqUri.parse(uriText);

            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");

            int expectedPort = ((port != null) ? port
                : (secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT));
            
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected");
            assertEquals((port != null), uri.hasPort(), "Port flag is not as expected");
            assertEquals(secure, uri.isSecure(), "Secure flag not as expected");
            assertEquals(user, uri.getUser(), "User is not as expected");
            assertEquals(password, uri.getPassword(), "Password is not as expected");            
            assertEquals(host, uri.getHost(), "Host is not as expected");
            assertEquals(virtualHost, uri.getVirtualHost(), 
                         "Virtual host is not as expected");
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected");

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected");

            assertEquals(uriText, uri.toString(),
                         "Result from toString() not as expected.");

            ConnectionUri baseUri = ConnectionUri.parse(uriText);
            assertInstanceOf(uri.getClass(), baseUri,
                             "Unexpected base URI parse: " + uriText);
            assertEquals(uri, baseUri, 
                         "Unexpected base URI parse: " + uriText);

        } catch (Exception e) {
            fail("Failed parsing with exception.", e);
        }        
    }

    private int changeIndex = 0;

    private synchronized int nextChangeIndex(int count) {
        return ((changeIndex++) % count);
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHash(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean secure2     = secure;
            String  user2       = user;
            String  password2   = password;
            String  host2       = host;

            int changeIndex = nextChangeIndex(4);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(secure2, user2, password2, host2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithVirtualHost(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host, virtualHost);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host, virtualHost);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean secure2         = secure;
            String  user2           = user;
            String  password2       = password;
            String  host2           = host;
            String  virtualHost2    = virtualHost;

            int changeIndex = nextChangeIndex(5);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                case 4:
                    virtualHost2 = ((virtualHost == null) ? "" : virtualHost) + "phoo";
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(secure2, user2, password2, host2, virtualHost2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithPort(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host, port);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host, port);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean secure2     = secure;
            String  user2       = user;
            String  password2   = password;
            String  host2       = host;
            Integer port2       = port;
            
            int changeIndex = nextChangeIndex(5);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                case 4:
                    port2 = ((port == null) ? 1000 : port) + 1;
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(secure2, user2, password2, host2, port2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithVirtualHostAndPort(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host, port, virtualHost);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host, port, virtualHost);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean secure2         = secure;
            String  user2           = user;
            String  password2       = password;
            String  host2           = host;
            Integer port2           = port;
            String  virtualHost2    = virtualHost;
            
            int changeIndex = nextChangeIndex(6);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                case 4:
                    port2 = ((port == null) ? 1000 : port) + 1;
                    break;
                case 5:
                    virtualHost2 = ((virtualHost == null) ? "" : virtualHost) + "phoo";
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(
                secure2, user2, password2, host2, port2, virtualHost2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithPortAndQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host, port, queryOptions);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host, port, queryOptions);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean             secure2         = secure;
            String              user2           = user;
            String              password2       = password;
            String              host2           = host;
            Integer             port2           = port;
            Map<String, String> queryOptions2   = queryOptions;
            
            int changeIndex = nextChangeIndex(6);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                case 4:
                    port2 = ((port == null) ? 1000 : port) + 1;
                    break;
                case 5:
                    queryOptions2 = ((queryOptions == null) ? new TreeMap<>()
                                     : new TreeMap<>(queryOptions));
                    queryOptions2.put(randomAlphabeticText(3), 
                                      randomAlphabeticText(3, 5));
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(
                secure2, user2, password2, host2, port2, queryOptions2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithVirtualHostAndQuery(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host, virtualHost, queryOptions);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host, virtualHost, queryOptions);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean             secure2         = secure;
            String              user2           = user;
            String              password2       = password;
            String              host2           = host;
            String              virtualHost2    = virtualHost;
            Map<String, String> queryOptions2   = queryOptions;
            
            int changeIndex = nextChangeIndex(6);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                case 4:
                    virtualHost2 = ((virtualHost == null) ? "" : virtualHost) + "phoo";
                    break;
                case 5:
                    queryOptions2 = ((queryOptions == null) ? new TreeMap<>()
                                     : new TreeMap<>(queryOptions));
                    queryOptions2.put(randomAlphabeticText(3), 
                                      randomAlphabeticText(3, 5));
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(
                secure2, user2, password2, host2, virtualHost2, queryOptions2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHashWithAll(
        boolean             secure,
        String              user,
        String              password,
        String              host,
        Integer             port,
        String              virtualHost,
        Map<String, String> queryOptions)
    {
        try {
            RabbitMqUri uri1 = new RabbitMqUri(
                secure, user, password, host, port, virtualHost, queryOptions);

            RabbitMqUri uri2 = new RabbitMqUri(
                secure, user, password, host, port, virtualHost, queryOptions);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            boolean             secure2         = secure;
            String              user2           = user;
            String              password2       = password;
            String              host2           = host;
            Integer             port2           = port;
            String              virtualHost2    = virtualHost;
            Map<String, String> queryOptions2   = queryOptions;
            
            int changeIndex = nextChangeIndex(7);
            switch (changeIndex) {
                case 0:
                    secure2 = !secure;
                    break;
                case 1:
                    user2 = user + "foo";
                    break;
                case 2:
                    password2 = password + "bar";
                    break;
                case 3:
                    host2 = host + "bax";
                    break;
                case 4:
                    port2 = ((port == null) ? 1000 : port) + 1;
                    break;
                case 5:
                    queryOptions2 = ((queryOptions == null) ? new TreeMap<>()
                                     : new TreeMap<>(queryOptions));
                    queryOptions2.put(randomAlphabeticText(3), 
                                      randomAlphabeticText(3, 5));
                    break;
                case 6:
                    virtualHost2 = ((virtualHost == null) ? "" : virtualHost) + "phoo";
                    break;
                default:
                    fail("Unrecognized change index: " + changeIndex);
            }

            uri2 = new RabbitMqUri(
                secure2, user2, password2, host2, port2, virtualHost2, queryOptions2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with exception.", e);
        }
    }

}
