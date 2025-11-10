package com.senzing.datamart;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Represents an SQS connection URI.
 */
public class SqsUri extends ConnectionUri {
    /**
     * The default port for HTTP insecure communication.
     */
    public static final int DEFAULT_PORT = 80;

    /**
     * The default port for HTTPS secure communication.
     */
    public static final int DEFAULT_SECURE_PORT = 443;

    /**
     * The prefix for the URI when using insecure communication including
     * the <code>"://sqs."</code>.
     */
    public static final String SCHEME_PREFIX = "http://sqs.";

    /**
     * The prefix for the URI when using secure communication including 
     * the <code>"://sqs."</code>.
     */
    public static final String SECURE_SCHEME_PREFIX = "https://sqs.";

    /**
     * Flag indicating if the URI indicates secure communication.
     */
    private boolean secure;

    /**
     * The AWS host.
     */
    private String host;

    /**
     * The AWS port.
     */
    private Integer port;

    /** 
     * The AWS path for the queue.
     */
    private String path;

    /**
     * The underlying {@link URI}.
     */
    private URI uri;

    /**
     * Constructs with the specified {@link URI}.
     * 
     * @param uri The non-null {@link URI} with which to connect.
     * 
     * @throws NullPointerException If the specified {@link URI}
     *                              is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public SqsUri(URI uri)
    {
        super(uri.getScheme().equalsIgnoreCase("https") 
              ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX,
              parseQueryOptions(uri.getQuery()));

        requireNonNull(uri, "The URI cannot be null");

        // get the string specification of the URI
        String spec = uri.toString();
        if (!spec.toLowerCase().startsWith(SECURE_SCHEME_PREFIX)
            && !spec.toLowerCase().startsWith(SCHEME_PREFIX))
        {
            throw new IllegalArgumentException(
                "The specified URI does not appear to be an SQS URI: " + spec);
        }

        // set the fields
        this.secure = uri.getScheme().equalsIgnoreCase("https");
        this.host   = uri.getHost();
        this.port   = (uri.getPort() > 0) ? uri.getPort() : null;
        this.path   = uri.getPath();
    }

    /**
     * Checks if this instance describes a secure connection to SQS.
     * 
     * @return <code>true</code> if this instance describes a secure
     *         connection to SQS, otherwise <code>false</code>.
     */
    public boolean isSecure() {
        return this.secure;
    }

    /**
     * Gets the host for the SQS connection.
     * 
     * @return The host for the SQS connection.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Gets the port for the SQS connection.
     * If the port number was not provided during construction
     * then this returns {@link #DEFAULT_PORT} for insecure
     * communication and {@link #DEFAULT_SECURE_PORT} for secure
     * communication as determined by {@link #isSecure()}.
     * 
     * @return The port for the SQS connection.
     */
    public int getPort() {
        return (this.port != null 
                ? this.port
                : (this.secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT));
    }

    /**
     * Checks if this instance had an explicit port specified or
     * if no port was provided, indicating the {@linkplain #DEFAULT_PORT
     * default port} or {@linkplain #DEFAULT_SECURE_PORT default secure 
     * port} should be used.
     * 
     * @return <code>true</code> if this instance had explicit port
     *         specified, or <code>false</code> if the port number is
     *         excluded, indicating the {@linkplain #DEFAULT_PORT
     *         default port} or {@linkplain #DEFAULT_SECURE_PORT 
     *         default secure port} should be used.
     */
    public boolean hasPort() {
        return (this.port != null);
    }

    /**
     * Gets the path for the SQS connection.
     * 
     * @return The path for the SQS connection.
     */
    public String getPath() {
        return this.path;
    }

    /**
     * Gets the {@link URI} for the SQS connection.
     * 
     * @return The {@link URI} for the SQS connection.
     */
    public URI getUri() {
        return this.uri;
    }

    /**
     * Implemented to format the URI as a {@link String} with proper
     * URI encoding.
     * 
     * @return The formatted URI for this instance.
     */
    public String toString() {
        return this.getUri().toString();
    }

    /**
     * Implemented to return a hash code consistent with the
     * {@link #equals(Object)} implementation.
     * 
     * @return The hash code for this instance.
     */
    public int hashCode() {
        return Objects.hash(this.isSecure(),
                            this.getHost(),
                            (this.hasPort() ? this.getPort() : null),
                            this.getPath(),
                            this.getUri());
    }

    /**
     * Implemented to return <code>true</code> if and only if the
     * specified parameter is a non-null reference to an object of the
     * same class with equivalent properties.
     * 
     * <p>
     * <b>NOTE:</b> An explicitly specified default port is <b>NOT</b>
     * considered equal to excluded/unspecified (<code>null</code>) 
     * port number.
     * </p>
     *  
     * @param obj The object to compare with.
     * 
     * @return <code>true</code> if and only if the objects are equal,
     *         otherwise <code>false</code>.
     */
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (this.getClass() != obj.getClass()) return false;
        SqsUri url = (SqsUri) obj;
        return Objects.equals(this.getUri(), url.getUri())
            && Objects.equals(this.isSecure(), url.isSecure())
            && Objects.equals(this.getHost(), url.getHost())
            && Objects.equals(this.hasPort(), url.hasPort())
            && Objects.equals(this.getPort(), url.getPort())
            && Objects.equals(this.getPath(), url.getPath());
    }

    /**
     * Provides a static factory method for parsing an SQS
     * connection URI formatted as a {@link String}.
     * 
     * @param uri The URI to parse.
     * 
     * @return The newly constructed {@link SqsUri} instance.
     * 
     * @throws NullPointerException If the specified parameter is
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified URI is not properly
     *                                  formatted.
     */
    public static SqsUri parse(String uri) {
        try {
            return new SqsUri(new URI(uri));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid SQS URI: " + uri, e);
        }
    }

    static {
        registerConnectionType(SCHEME_PREFIX, SqsUri.class);
        registerConnectionType(SECURE_SCHEME_PREFIX, SqsUri.class);
    }
}
