package com.senzing.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.text.TextUtilities.*;
import static com.senzing.datamart.SqsUri.*;

/**
 * Tests for {@link SqsUri}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqsUriTest {

    @Test
    public void testNullParameter()
    {
        try {
            new SqsUri(null);
            
            fail("Unexpectedly succeeded in constructing with "
                + "a null URI.");

        } catch (NullPointerException expected) {
            // expected
        }
    }

    public List<Arguments> getConstructParameters() {
        try {
            List<Arguments> result = new LinkedList<>();

            result.add(Arguments.of(
                new URI("http://sqs.us-east-2.amazonaws.com/123456/MyQueue"),
                false, // not secure
                "sqs.us-east-2.amazonaws.com",
                DEFAULT_PORT,
                false, // does not have port
                "/123456/MyQueue",
                null, // query options
                null)); // no exception

            result.add(Arguments.of(
                new URI("https://sqs.us-east-1.amazonaws.com/654321/SomeQueue"),
                true, // secure
                "sqs.us-east-1.amazonaws.com",
                DEFAULT_SECURE_PORT,
                false, // does not have port
                "/654321/MyQueue",
                null, // query options
                null)); // no exception

            result.add(Arguments.of(
                new URI("http://sqs.us-east-2.amazonaws.com/123456/MyQueue?foo=bar"),
                false, // not secure
                "sqs.us-east-2.amazonaws.com",
                DEFAULT_PORT,
                false, // does not have port
                "/123456/MyQueue",
                Map.of("foo", "bar"), // query options
                null)); // no exception

            result.add(Arguments.of(
                new URI("https://sqs.us-east-1.amazonaws.com/654321/SomeQueue?phoo=bax"),
                true, // secure
                "sqs.us-east-1.amazonaws.com",
                DEFAULT_SECURE_PORT,
                false, // does not have port
                "/654321/MyQueue", // path
                Map.of("phoo", "bax"), // query options
                null)); // no exception

            result.add(Arguments.of(
                new URI("http://sqs.us-east-2.amazonaws.com:80/123456/MyQueue"),
                false, // not secure
                "sqs.us-east-2.amazonaws.com",
                80,   // port
                true, // does not have port
                "/123456/MyQueue", // path
                null, // query options
                null)); // no exception

            result.add(Arguments.of(
                new URI("https://sqs.us-east-1.amazonaws.com:443/654321/SomeQueue"),
                true, // secure
                "sqs.us-east-1.amazonaws.com",
                443,
                true, // does not have port
                "/654321/MyQueue",
                null, // query options
                null)); // no exception

            result.add(Arguments.of(
                new URI("http://sqs.us-east-2.amazonaws.com:8080/123456/MyQueue"),
                false, // not secure
                "sqs.us-east-2.amazonaws.com",
                8080,
                true, // does not have port
                "/123456/MyQueue",
                null, // query options
                null)); // no exception
            
            result.add(Arguments.of(
                new URI("https://sqs.us-east-1.amazonaws.com:789/654321/SomeQueue"),
                true, // secure
                "sqs.us-east-1.amazonaws.com",
                789,
                true, // does not have port
                "/654321/MyQueue",
                null, // query options
                null)); // no exception
                
            result.add(Arguments.of(
                new URI("http://www.amazon.com/13579/NotAQueue"),
                null, // secure
                null, // host
                null, // port
                null, // has-port
                null, // path
                null, // query options
                IllegalArgumentException.class)); // no exception

            result.add(Arguments.of(
                new URI("https://www.google.com/987654/NotAQueue"),
                null, // secure
                null, // host
                null, // port
                null, // has-port
                null, // path
                null, // query options
                IllegalArgumentException.class)); // no exception
                
            return result;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testConstructor(URI                         httpUri,
                                Boolean                     secure,
                                String                      host,
                                Integer                     port,
                                Boolean                     hasPort,
                                String                      path,
                                Map<String, String>         queryOptions,
                                Class<? extends Exception>  exceptionClass)
    {
        SqsUri uri = null;

        try {
            uri = new SqsUri(httpUri);

            if (exceptionClass != null) {
                fail("Unexpectedly succeeded when an exception "
                     + "was expected.  exception=[ " + exceptionClass
                     + " ], uri=[ " + httpUri + " ]");
            }
        } catch (Exception e) {
            if (exceptionClass == null 
                || !exceptionClass.isAssignableFrom(e.getClass()))
            {
                fail("Failed test with unexpected exception: " + httpUri, e);
            } else {
                // received expected exception
                return;
            }
        }

        try {
            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected: " + httpUri);
            assertEquals(hasPort, uri.hasPort(), 
                         "Port flag is not as expected: " + httpUri);
            assertEquals(secure, uri.isSecure(),
                         "Secure flag not as expected: " + httpUri);
            assertEquals(host, uri.getHost(), 
                         "User is not as expected: " + httpUri);
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected: " + httpUri);

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected; " + httpUri);
            assertEquals(httpUri, uri.getUri(), "URI is not as expected");

        } catch (Exception e) {
            fail("Failed test with unexpected exception: " + httpUri, e);
        }
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testParse(URI                           httpUri,
                          Boolean                       secure,
                          String                        host,
                          Integer                       port,
                          Boolean                       hasPort,
                          String                        path,
                          Map<String, String>           queryOptions,
                          Class<? extends Exception>    exceptionClass)
    {
        SqsUri uri = null;

        String uriText = httpUri.toASCIIString();

        try {
            uri = SqsUri.parse(uriText);

            if (exceptionClass != null) {
                fail("Unexpectedly succeeded when an exception "
                     + "was expected.  exception=[ " + exceptionClass
                     + " ], uri=[ " + httpUri + " ]");
            }

        } catch (Exception e) {
            if (exceptionClass == null 
                || !exceptionClass.isAssignableFrom(e.getClass()))
            {
                fail("Failed test with unexpected exception: " + httpUri, e);
            } else {
                // received expected exception
                return;
            }
        }

        try {        
            int expectedPort = (port != null) ? port
                : ((secure) ? DEFAULT_SECURE_PORT : DEFAULT_PORT);
            Map<String, String> expectedOptions = (queryOptions == null)
                ? Collections.emptyMap() : queryOptions;
            
            assertEquals(expectedPort, uri.getPort(),
                         "Port was not as expected: " + httpUri);
            assertEquals(hasPort, uri.hasPort(), 
                         "Port flag is not as expected: " + httpUri);
            assertEquals(secure, uri.isSecure(), 
                         "Secure flag not as expected: " + httpUri);
            assertEquals(host, uri.getHost(), 
                         "User is not as expected: " + httpUri);
            assertEquals(expectedOptions, uri.getQueryOptions(),
                         "Query options are not as expected: " + httpUri);

            String expectedScheme = (secure) ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX;
            assertEquals(expectedScheme, uri.getSchemePrefix(),
                         "Scheme prefix is not as expected: " + httpUri);
            assertEquals(httpUri, uri.getUri(), 
                         "URI is not as expected: " + httpUri);
            assertEquals(uriText, uri.toString(),
                         "Result from toString() not as expected: "
                        + httpUri);

            ConnectionUri baseUri = ConnectionUri.parse(uriText);
            assertInstanceOf(uri.getClass(), baseUri,
                             "Unexpected base URI parse: " + uriText);
            assertEquals(uri, baseUri, 
                         "Unexpected base URI parse: " + uriText);

        } catch (Exception e) {
            fail("Failed test with unexpected exception: " + httpUri, e);
        }        
    }

    @ParameterizedTest
    @MethodSource("getConstructParameters")
    public void testEqualsAndHash(URI                           httpUri,
                                  Boolean                       secure,
                                  String                        host,
                                  Integer                       port,
                                  Boolean                       hasPort,
                                  String                        path,
                                  Map<String, String>           queryOptions,
                                  Class<? extends Exception>    exceptionClass)
    {
        SqsUri uri1 = null;
        SqsUri uri2 = null;
        String uriText = httpUri.toASCIIString();

        try {
            uri1 = new SqsUri(httpUri);

            if (exceptionClass != null) {
                fail("Unexpectedly succeeded when an exception "
                     + "was expected.  exception=[ " + exceptionClass
                     + " ], uri=[ " + httpUri + " ]");
            }
        } catch (Exception e) {
            if (exceptionClass == null 
                || !exceptionClass.isAssignableFrom(e.getClass()))
            {
                fail("Failed test with unexpected exception: " + httpUri, e);
            } else {
                // received expected exception
                return;
            }
        }


        try {
            uri2 = new SqsUri(httpUri);

            assertEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");

            String prefix = (queryOptions != null && queryOptions.size() > 0) ? "&" : "?";

            uriText = uriText + prefix + urlEncodeUtf8(randomPrintableText(3))
                + "=" + urlEncodeUtf8(randomPrintableText(4, 8));

            URI httpUri2 = new URI(uriText);

            uri2 = new SqsUri(httpUri2);

            assertNotEquals(uri1, uri2, "Objects are unexpectedly not equal");
            assertNotEquals(uri1.hashCode(), uri2.hashCode(),
                "Objects unexpectedly have different hash codes");
            
        } catch (Exception e) {
            fail("Failed test with unexpected exception: " + uriText, e);
        }
        
    }
}
