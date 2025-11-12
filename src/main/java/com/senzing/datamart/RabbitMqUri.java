package com.senzing.datamart;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;

import static com.senzing.text.TextUtilities.urlDecodeUtf8;
import static com.senzing.text.TextUtilities.urlEncodeUtf8;

/**
 * Represents a RabbitMQ connection URI.
 */
public class RabbitMqUri extends ConnectionUri {
    /**
     * The default port for RabbitMQ insecure communication.
     */
    public static final int DEFAULT_PORT = 5672;

    /**
     * The default port for RabbitMQ secure communication.
     */
    public static final int DEFAULT_SECURE_PORT = 5671;

    /**
     * The prefix for the URI when using insecure communication including
     * the <code>"://"</code>.
     */
    public static final String SCHEME_PREFIX = "amqp://";

    /**
     * The prefix for the URI when using secure communication including 
     * the <code>"://"</code>.
     */
    public static final String SECURE_SCHEME_PREFIX = "amqps://";

    /**
     * Flag indicating if the scheme indicates secure communication.
     */
    private boolean secure;

    /**
     * The user with which to authenticate.
     */
    private String user;

    /**
     * The password with which to authenticate.
     */
    private String password;

    /**
     * The RabbitMQ host.
     */
    private String host;

    /**
     * The RabbitMQ port.
     */
    private Integer port;

    /**
     * The virtual host name.
     */
    private String virtualHost;

    /**
     * Constructs with the specified parameters.  No port
     * number is specified in this constructor, indicating
     * that the {@linkplain #DEFAULT_PORT default port}
     * should be used for insecure communication and
     * {@linkplain #DEFAULT_SECURE_PORT default secure port}
     * should be used for secure communication.
     * 
     * @param secure <code>true</code> if the connection to
     *               RabbitMQ should be secure, otherwise 
     *               <code>false</code>.
     * @param user The user with which to authenticate.
     * @param password The user with which to authenticate.
     * @param host THe host for the RabbitMQ server.
     * 
     * @throws NullPointerException If any of the parameters
     *                              is <code>null</code>.
     */
    public RabbitMqUri(boolean  secure,
                       String   user,
                       String   password,
                       String   host)
    {
        this(secure, user, password, host, (String) null);
    }

    /**
     * Constructs with the specified parameters.  No port
     * number is specified in this constructor, indicating
     * that the {@linkplain #DEFAULT_PORT default port}
     * should be used for insecure communication and
     * {@linkplain #DEFAULT_SECURE_PORT default secure port}
     * should be used for secure communication.
     * 
     * @param secure <code>true</code> if the connection to
     *               RabbitMQ should be secure, otherwise 
     *               <code>false</code>.
     * @param user The user with which to authenticate.
     * @param password The user with which to authenticate.
     * @param host THe host for the RabbitMQ server.
     * @param virtualHost The optional virtual host name, or
     *                    <code>null</code> if no virtual host.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     */
    public RabbitMqUri(boolean  secure,
                       String   user,
                       String   password,
                       String   host,
                       String   virtualHost)
    {
        this(secure, user, password, host, null, virtualHost);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.
     * 
     * @param secure <code>true</code> if secure communication should be
     *               used for the connection, otherwise <code>false</code>.
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the RabbitMQ server.
     * @param port The port for the RabbitMQ server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public RabbitMqUri(boolean  secure,
                       String   user,
                       String   password,
                       String   host,
                       Integer  port)
    {
        this(secure, user, password, host, port, (String) null);
    }

    /**
     * Constructs with the specified parameters.
     * 
     * @param secure <code>true</code> if secure communication should be
     *               used for the connection, otherwise <code>false</code>.
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the RabbitMQ server.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public RabbitMqUri(boolean              secure,
                       String               user,
                       String               password,
                       String               host,
                       Map<String, String>  queryOptions)
    {
        this(secure, user, password, host, null, (String) null, queryOptions);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.
     * 
     * @param secure <code>true</code> if secure communication should be
     *               used for the connection, otherwise <code>false</code>.
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the RabbitMQ server.
     * @param port The port for the RabbitMQ server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * @param virtualHost The optional virtual host name, or
     *                    <code>null</code> if no virtual host.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public RabbitMqUri(boolean  secure,
                       String   user,
                       String   password,
                       String   host,
                       Integer  port,
                       String   virtualHost)
    {
        this(secure, user, password, host, port, virtualHost, null);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.
     * 
     * @param secure <code>true</code> if secure communication should be
     *               used for the connection, otherwise <code>false</code>.
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the RabbitMQ server.
     * @param port The port for the RabbitMQ server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public RabbitMqUri(boolean              secure,
                       String               user,
                       String               password,
                       String               host,
                       Integer              port,
                       Map<String,String>   queryOptions)
    {
        this(secure, user, password, host, port, null, queryOptions);
    }


    /**
     * Constructs with the specified parameters.
     * 
     * @param secure <code>true</code> if secure communication should be
     *               used for the connection, otherwise <code>false</code>.
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the RabbitMQ server.
     * @param virtualHost The optional virtual host name, or
     *                    <code>null</code> if no virtual host.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public RabbitMqUri(boolean              secure,
                       String               user,
                       String               password,
                       String               host,
                       String               virtualHost,
                       Map<String,String>   queryOptions)
    {
        this(secure, user, password, host, null, virtualHost, queryOptions);
    }
    

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.
     * 
     * @param secure <code>true</code> if secure communication should be
     *               used for the connection, otherwise <code>false</code>.
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the RabbitMQ server.
     * @param port The port for the RabbitMQ server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * @param virtualHost The optional virtual host name, or
     *                    <code>null</code> if no virtual host.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If any of the required 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public RabbitMqUri(boolean              secure,
                       String               user,
                       String               password,
                       String               host,
                       Integer              port,
                       String               virtualHost,
                       Map<String,String>   queryOptions)
    {
        super((secure ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX), queryOptions);

        requireNonNull(user, "The user cannot be null");
        requireNonNull(password, "The password cannot be null");
        requireNonNull(host, "The host cannot be null");
        if (port != null && port <= 0) {
            throw new IllegalArgumentException(
                "The specified port must be a positive integer: "
                + port);
        }

        // set the fields
        this.secure         = secure;
        this.user           = user;
        this.password       = password;
        this.host           = host;
        this.port           = port;
        this.virtualHost    = virtualHost;
    }

    /**
     * Checks if this instance describes a secure connection to RabbitMQ.
     * 
     * @return <code>true</code> if this instance describes a secure
     *         connection to RabbitMQ, otherwise <code>false</code>.
     */
    public boolean isSecure() {
        return this.secure;
    }

    /**
     * Gets the user name for the RabbitMQ connection.
     * 
     * @return The user name for the RabbitMQ connection.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Gets the password for the RabbitMQ connection.
     * 
     * @return The password for the RabbitMQ connection.
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Gets the host for the RabbitMQ connection.
     * 
     * @return The host for the RabbitMQ connection.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Gets the port for the RabbitMQ connection.
     * If the port number was not provided during construction
     * then this returns {@link #DEFAULT_PORT} for insecure
     * communication and {@link #DEFAULT_SECURE_PORT} for secure
     * communication as determined by {@link #isSecure()}.
     * 
     * @return The port for the RabbitMQ connection.
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
     * Gets the virtual host name for the RabbitMQ connection.
     * 
     * @return The virtual host name for the RabbitMQ connection.
     */
    public String getVirtualHost() {
        return this.virtualHost;
    }

    /**
     * Implemented to format the URI as a {@link String} with proper
     * URI encoding.
     * 
     * @return The formatted URI for this instance.
     */
    public String toString() {
        String prefix = (this.isSecure() 
            ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX);

        return prefix + urlEncodeUtf8(this.getUser()) + ":" 
            + urlEncodeUtf8(this.getPassword())
            + "@" + urlEncodeUtf8(this.getHost()) 
            + (this.hasPort() ? (":" + this.getPort()) : "")
            + "/" 
            + ((this.getVirtualHost() == null)
                ? "" : urlEncodeUtf8(this.getVirtualHost())
            + this.getQueryString());
    }

    /**
     * Implemented to return a hash code consistent with the
     * {@link #equals(Object)} implementation.
     * 
     * @return The hash code for this instance.
     */
    public int hashCode() {
        return Objects.hash(this.isSecure(),
                            this.getUser(), 
                            this.getPassword(),
                            this.getHost(),
                            (this.hasPort() ? this.getPort() : null),
                            this.getVirtualHost(),
                            this.getQueryOptions());

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
        RabbitMqUri uri = (RabbitMqUri) obj;
        return Objects.equals(this.isSecure(), uri.isSecure())
            && Objects.equals(this.getUser(), uri.getUser())
            && Objects.equals(this.getPassword(), uri.getPassword())
            && Objects.equals(this.getHost(), uri.getHost())
            && Objects.equals(this.hasPort(), uri.hasPort())
            && Objects.equals(this.getPort(), uri.getPort())
            && Objects.equals(this.getVirtualHost(), uri.getVirtualHost())
            && Objects.equals(this.getQueryOptions(), uri.getQueryOptions());
    }

    /**
     * Provides a static factory method for parsing a RabbitMQ
     * connection URI formatted as a {@link String}.
     * 
     * @param uri The URI to parse.
     * 
     * @return The newly constructed {@link RabbitMqUri} instance.
     * 
     * @throws NullPointerException If the specified parameter is
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified URI is not properly
     *                                  formatted.
     */
    public static RabbitMqUri parse(String uri) {
        requireNonNull(uri, "The URI cannot be null");

        // check the prefix
        if (!(uri.toLowerCase().startsWith(SCHEME_PREFIX) 
              && uri.length() > SCHEME_PREFIX.length())
            && !(uri.toLowerCase().startsWith(SECURE_SCHEME_PREFIX) 
                 && uri.length() > SECURE_SCHEME_PREFIX.length()))
        {
            throw new IllegalArgumentException(
                "URI must begin with " + SCHEME_PREFIX + " or " 
                + SECURE_SCHEME_PREFIX + ": " + uri);
        }

        // check if secure
        boolean secure = uri.toLowerCase().startsWith(SECURE_SCHEME_PREFIX);

        // determine which prefix to use
        String prefix = (secure ? SECURE_SCHEME_PREFIX : SCHEME_PREFIX);

        // get the suffix
        String suffix = uri.substring(prefix.length());

        // find the user
        String user = null;
        int index = suffix.indexOf(':');
        if (index <= 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Unable to parse user name from uri: " + uri);
        }
        user = urlDecodeUtf8(suffix.substring(0, index).trim());
        
        // get the suffix
        suffix = suffix.substring(index+1);

        // find the password
        String password = null;
        index = suffix.indexOf('@');
        if (index < 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Unable to parse password from uri: " + uri);
        }
        password = urlDecodeUtf8(suffix.substring(0, index));

        // get the suffix
        suffix = suffix.substring(index + 1);

        // find the host and optional port
        String host = null;
        boolean hasPort = true;
        index = suffix.indexOf(':');
        if (index < 0) {
            hasPort = false;
            index = suffix.indexOf('/');
        }
        if (index == 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Unable to parse host from uri: " + uri);
        }
        host = urlDecodeUtf8(suffix.substring(0, index).trim());

        // get the suffix
        suffix = suffix.substring(index + 1);

        // check if we have a port
        Integer port = null;
        if (hasPort) {
            index = suffix.indexOf('/');
            if (index <= 0) {
                throw new IllegalArgumentException(
                    "Unable to parse port from uri: " + uri);
            }

            String portText = urlDecodeUtf8(suffix.substring(0, index).trim());
            try {
                port = Integer.parseInt(portText);

                if (port <= 0) {
                    throw new IllegalArgumentException(
                        "The port number must be a positive integer: " + port);
                }

            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Unable to parse port from uri: " + uri, e);
            }

            // get the suffix
            suffix = (index < suffix.length() - 1) ? suffix.substring(index + 1) : "";

        } else {
            // set the port to null
            port = null;
        }

        // get the virtualHost
        String virtualHost = null;
        index = suffix.indexOf('?');
        if (index < 0) {
            virtualHost = urlDecodeUtf8(suffix.trim());
            suffix = null;

        } else {
            // check if we have a virtual host
            if (index > 0) {
                virtualHost = urlDecodeUtf8(suffix.substring(0, index).trim());
            }
            suffix = (index == suffix.length() - 1) ? "" : suffix.substring(index+1);
        }

        // parse the query parameters if found
        Map<String,String> queryOptions = ConnectionUri.parseQueryOptions(suffix);

        // construct the instance
        return new RabbitMqUri(
            secure, user, password, host, port, virtualHost, queryOptions);
    }

    static {
        registerConnectionType(SCHEME_PREFIX, RabbitMqUri.class);
        registerConnectionType(SECURE_SCHEME_PREFIX, RabbitMqUri.class);
    }
}
