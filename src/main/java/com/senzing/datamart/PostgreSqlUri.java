package com.senzing.datamart;

import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.senzing.text.TextUtilities.urlDecodeUtf8;
import static com.senzing.text.TextUtilities.urlEncodeUtf8;

/**
 * Represents a PostgreSQL database connection URI.
 */
public class PostgreSqlUri extends ConnectionUri {
    /**
     * The default port for the PostgreSQL database connection.
     * The value of this constant is <code>{@value}</code>.
     */
    public static final int DEFAULT_PORT = 5432;

    /**
     * The default schema for the PostgreSQL database connection.
     * The value of this constant is <code>{@value}</code>.
     */
    public static final String DEFAULT_SCHEMA = "public";

    /**
     * The expected format for this URI (<code>{@value}</code>).
     */
    public static final String EXPECTED_FORMAT
        = "postgresql://user:password@host:port:database/?schema=schemaName";

    /**
     * The query options key for the schema.
     */
    public static final String SCHEMA_KEY = "schema";

    /**
     * The scheme prefix for the URI's including the <code>"://"</code>.
     */
    public static final String SCHEME_PREFIX = "postgresql://";

    /**
     * The user with which to authenticate.
     */
    private String user;

    /**
     * The password with which to authenticate.
     */
    private String password;

    /**
     * The database host.
     */
    private String host;

    /**
     * The database port, or <code>null</code> if using the {@link #DEFAULT_PORT}.
     */
    private Integer port;

    /**
     * The database name.
     */
    private String database;

    /**
     * Makes a query options {@link Map} that merges the option for the
     * schema using {@link #SCHEMA_KEY} with the other options (which may 
     * be <code>null</code>).
     * 
     * @param schema The non-null schema name.
     * @param queryOptions The other query options, or <code>null</code> if none.
     * 
     * @return The merged query options {@link Map} of {@link String} keys to 
     *         {@link String} values with the {@link #SCHEMA_KEY} first.
     */
    private static Map<String,String> makeQueryOptions(String               schema, 
                                                       Map<String,String>   queryOptions)
    {
        // if no explicit schema, then return the query options as specified
        if (schema == null) {
            return queryOptions;
        }
        // create the result map
        Map<String,String> result = new LinkedHashMap<>();

        // add the schema
        result.put(SCHEMA_KEY, schema);

        // add every other query option except the schema
        queryOptions.forEach((key, value) -> {
            // we already handled the schema key, so skip schema
            if (!SCHEMA_KEY.equalsIgnoreCase(key)) {
                result.put(key, value);
            }
        });

        // return the result
        return result;
    }

    /**
     * Constructs with the specified parameters.  The port number
     * is not specified so the {@linkplain #DEFAULT_PORT default port}
     * will be used.  The schema is not specified so the {@linkplain
     * #DEFAULT_SCHEMA default schema} will be used.
     * 
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the database server.
     * @param database The non-null database name.
     * 
     * @throws NullPointerException If any of the required {@link String} 
     *                              parameters is <code>null</code>.
     */
    public PostgreSqlUri(String user,
                         String password,
                         String host,
                         String database)
    {
        this(user, password, host, (Integer) null, database);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.  The
     * schema is not specified so the {@linkplain #DEFAULT_SCHEMA
     * default schema} will be used.
     * 
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the database server.
     * @param port The port for the database server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * @param database The non-null database name.
     * 
     * @throws NullPointerException If any of the required {@link String} 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public PostgreSqlUri(String     user,
                         String     password,
                         String     host,
                         Integer    port,
                         String     database)
    {
        this(user, password, host, port, database, null);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * is not specified so the {@linkplain #DEFAULT_PORT default port}
     * will be used.  If the schema is <code>null</code> the {@linkplain
     * #DEFAULT_SCHEMA default schema} will be used.
     * 
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the database server.
     * @param database The non-null database name.
     * @param schema The schema for the database, or <code>null</code>
     *               if the {@linkplain #DEFAULT_SCHEMA default schema} should
     *               be used.
     * 
     * @throws NullPointerException If any of the required {@link String} 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public PostgreSqlUri(String     user,
                         String     password,
                         String     host,
                         String     database,
                         String     schema)
    {
        this(user, password, host, null, database, schema);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.  If 
     * the schema is <code>null</code> the {@linkplain #DEFAULT_SCHEMA
     * default schema} will be used.
     * 
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the database server.
     * @param port The port for the database server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * @param database The non-null database name.
     * @param schema The schema for the database, or <code>null</code>
     *               if the {@linkplain #DEFAULT_SCHEMA default schema} should
     *               be used.
     * 
     * @throws NullPointerException If any of the required {@link String} 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public PostgreSqlUri(String     user,
                         String     password,
                         String     host,
                         Integer    port,
                         String     database,
                         String     schema)
    {
        this(user, password, host, port, database, schema, null);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * is not specified so the {@linkplain #DEFAULT_PORT default port}
     * will be used.  If the schema is <code>null</code> the
     * {@linkplain #DEFAULT_SCHEMA default schema} will be used.
     * 
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the database server.
     * @param database The non-null database name.
     * @param schema The schema for the database, or <code>null</code>
     *               if the {@linkplain #DEFAULT_SCHEMA default schema} should
     *               be used.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If any of the required {@link String} 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public PostgreSqlUri(String             user,
                         String             password,
                         String             host,
                         String             database,
                         String             schema,
                         Map<String,String> queryOptions)
    {
        this(user, password, host, null, database, schema, queryOptions);
    }

    /**
     * Constructs with the specified parameters.  The port number
     * may be specified as <code>null</code> indicating that the
     * {@linkplain #DEFAULT_PORT default port} should be used.  If
     * the schema is <code>null</code> the {@linkplain #DEFAULT_SCHEMA
     * default schema} will be used.
     * 
     * @param user The non-null user with which to authenticate.
     * @param password The non-null password with which to authenticate.
     * @param host THe non-null host for the database server.
     * @param port The port for the database server, or <code>null</code>
     *             if the {@linkplain #DEFAULT_PORT default port} should
     *             be used.
     * @param database The non-null database name.
     * @param schema The schema for the database, or <code>null</code>
     *               if the {@linkplain #DEFAULT_SCHEMA default schema} should
     *               be used.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If any of the required {@link String} 
     *                              parameters is <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified port is
     *                                  not a positive integer.
     */
    public PostgreSqlUri(String             user,
                         String             password,
                         String             host,
                         Integer            port,
                         String             database,
                         String             schema,
                         Map<String,String> queryOptions)
    {
        super(SCHEME_PREFIX, makeQueryOptions(schema, queryOptions));
        requireNonNull(user, "The user cannot be null");
        requireNonNull(password, "The password cannot be null");
        requireNonNull(host, "The host cannot be null");
        requireNonNull(database, "The database cannot be null");
        if (port <= 0) {
            throw new IllegalArgumentException(
                "The specified port must be a positive integer: "
                + port);
        }

        // set the fields
        this.user       = user;
        this.password   = password;
        this.host       = host;
        this.port       = port;
        this.database   = database;
    }

    /**
     * Gets the user name for the PostgreSQL database connection.
     * 
     * @return The user name for the PostgreSQL database connection.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Gets the password for the PostgreSQL database connection.
     * 
     * @return The password for the PostgreSQL database connection.
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Gets the host for the PostgreSQL database connection.
     * 
     * @return The host for the PostgreSQL database connection.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Gets the port for the PostgreSQL database connection.
     * This returns {@link #DEFAULT_PORT} if no port was provided.
     * 
     * @return The port for the PostgreSQL database connection.
     */
    public int getPort() {
        return (this.port == null) ? DEFAULT_PORT : this.port;
    }

    /**
     * Checks if this instance has an explicit port number, or
     * if the port number was omitted and the {@linkplain
     * #DEFAULT_PORT default port} is being used.
     * 
     * @return <code>true</code> if an explicit port number was
     *         provided, or <code>false</code> if not and the 
     *         {@linkplain #DEFAULT_PORT default port} is being used.
     */
    public boolean hasPort() {
        return (this.port != null);
    }

    /**
     * Gets the database name for the PostgreSQL database connection.
     * 
     * @return The database name for the PostgreSQL database connection.
     */
    public String getDatabase() {
        return this.database;
    }

    /**
     * Gets the schema name for the PostgreSQL database connection.
     * This returns {@link #DEFAULT_SCHEMA} if no schema was provided.
     * 
     * @return The schema name for the PostgreSQL database connection.
     */
    public String getSchema() {
        String schema = this.getQueryOptions().get(SCHEMA_KEY);
        return (schema == null) ? DEFAULT_SCHEMA : schema;
    }
    
    /**
     * Checks if this instance has an explicit schema, or if the
     * schema was omitted and the {@linkplain #DEFAULT_SCHEMA
     * default schema} is being used.
     * 
     * @return <code>true</code> if an explicit schema was provided,
     *         or <code>false</code> if not and the {@linkplain 
     *         #DEFAULT_SCHEMA default schema} is being used.
     */
    public boolean hasSchema() {
        return this.getQueryOptions().containsKey(SCHEMA_KEY);
    }

    /**
     * Implemented to format the URI as a {@link String} with proper
     * URI encoding.
     * 
     * @return The formatted URI for this instance.
     */
    public String toString() {
        return SCHEME_PREFIX + urlEncodeUtf8(this.getUser()) + ":" 
            + urlEncodeUtf8(this.getPassword())
            + "@" + urlEncodeUtf8(this.getHost())
            + (this.hasPort() ? (":" + this.getPort()) : "")
            + ":" + urlEncodeUtf8(this.getDatabase())
            + "/" + this.getQueryString();
    }

    /**
     * Implemented to return a hash code consistent with the
     * {@link #equals(Object)} implementation.
     * 
     * @return The hash code for this instance.
     */
    public int hashCode() {
        return Objects.hash(this.getUser(), 
                            this.getPassword(),
                            this.getHost(),
                            this.getPort(),
                            this.hasPort(),
                            this.getDatabase(),
                            this.getQueryOptions());
    }

    /**
     * Implemented to return <code>true</code> if and only if the specified
     * parameter is a non-null reference to an object of the same class with
     * equivalent properties.
     * 
     * <p>
     * <b>NOTE:</b> An explicitly specified {@linkplain #DEFAULT_PORT default port}
     * is <b>NOT</b> considered equal to excluded/unspecified (<code>null</code>) 
     * port number.
     * </p>
     *  
     * @param obj The object to compare with.
     * 
     * @return <code>true</code> if and only if the objects are equal, otherwise
     *         <code>false</code>.
     */
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (this.getClass() != obj.getClass()) return false;
        PostgreSqlUri url = (PostgreSqlUri) obj;
        return Objects.equals(this.getUser(), url.getUser())
            && Objects.equals(this.getPassword(), url.getPassword())
            && Objects.equals(this.getHost(), url.getHost())
            && Objects.equals(this.getPort(), url.getPort())
            && Objects.equals(this.hasPort(), url.hasPort())
            && Objects.equals(this.getDatabase(), url.getDatabase())
            && Objects.equals(this.getQueryOptions(), url.getQueryOptions());
    }

    /**
     * Provides a static factory method for parsing a PostgreSQL database
     * connection URI formatted as a {@link String}.  This supports parsing 
     * URI's defined in the format given by {@link #EXPECTED_FORMAT}.
     * <ul>
     *  <li><code>postgresql://user:password@host:port:database/?schema=schemaName</code></li>
     * </ul>
     * 
     * @param uri The URI to parse.
     * 
     * @return The newly constructed {@link PostgreSQLUrl} instance.
     * 
     * @throws NullPointerException If the specified parameter is
     *                              <code>null</code>.
     * @throws IllegalArgumentException If the specified URI is not properly
     *                                  formatted.
     */
    public static PostgreSqlUri parse(String uri) {
        requireNonNull(uri, "The URI cannot be null");

        // check the prefix
        if (!(uri.toLowerCase().startsWith(SCHEME_PREFIX) 
              && uri.length() > SCHEME_PREFIX.length())) 
        {
            throw new IllegalArgumentException(
                "URI must begin with \"" + SCHEME_PREFIX + "\": " + uri);
        }

        // get the suffix
        String suffix = uri.substring(SCHEME_PREFIX.length());

        // find the user
        int index = suffix.indexOf(':');
        if (index <= 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Formatting error while parsing user name from uri: uri=[ " 
                + uri + " ], expectedFormat=[ " + EXPECTED_FORMAT + " ]");
        }
        String user = urlDecodeUtf8(suffix.substring(0, index).trim()).trim();
        
        // get the suffix
        suffix = suffix.substring(index+1);

        // find the password
        index = suffix.indexOf('@');
        if (index < 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Formatting error while parsing password from uri: uri=[ " 
                + uri + " ], expectedFormat=[ " + EXPECTED_FORMAT + " ]");
        }
        String password = urlDecodeUtf8(suffix.substring(0, index));

        // get the suffix
        suffix = suffix.substring(index + 1);

        // find the host and optional port
        index = suffix.indexOf(':');
        if (index <= 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Formatting error while parsing host from uri: uri=[ " 
                + uri + " ], expectedFormat=[ " + EXPECTED_FORMAT + " ]");
        }
        String host = urlDecodeUtf8(suffix.substring(0, index).trim()).trim();

        // get the suffix
        suffix = suffix.substring(index + 1);

        // check if we have a port
        index = suffix.indexOf(':');
        if (index <= 0 || index == suffix.length() - 1) {
            throw new IllegalArgumentException(
                "Formatting error while parsing port from uri: uri=[ " 
                + uri + " ], expectedFormat=[ " + EXPECTED_FORMAT + " ]");
        }

        String portText = urlDecodeUtf8(suffix.substring(0, index).trim()).trim();
        boolean hasPort = true;
        for (char c : portText.toCharArray()) {
            if (c < '0' || c > '9') {
                hasPort = false;
                break;
            }
        }
        Integer port = null;
        if (hasPort) {
            // advance the suffix
            suffix = suffix.substring(index + 1);

            // parse the port
            try {
                port = Integer.parseInt(portText);

                if (port <= 0) {
                    throw new IllegalArgumentException(
                        "The port number must be a positive integer: " + port);
                }

            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Formatting error while parsing port from uri: uri=[ " 
                    + uri + " ], expectedFormat=[ " + EXPECTED_FORMAT + " ]", e);
            }
        }

        // get the database
        index = suffix.indexOf("/");
        if (index <= 0) {
            throw new IllegalArgumentException(
                "Formatting error while parsing database from uri: uri=[ " 
                + uri + " ], expectedFormat=[ " + EXPECTED_FORMAT + " ]");
        }
        String database = urlDecodeUtf8(suffix.substring(0, index).trim());

        // check if we are ending on the forward slash
        if (index == suffix.length() - 1) {
            suffix = "";
        } else {
            suffix = suffix.substring(index+1);
        }

        // parse the query parameters if found
        Map<String,String> queryOptions = ConnectionUri.parseQueryOptions(suffix);

        // get the schema name
        String schema = queryOptions.get(SCHEMA_KEY);

        // construct the instance
        return new PostgreSqlUri(
            user, password, host, port, database, schema, queryOptions);
    }

    static {
        registerConnectionType(SCHEME_PREFIX, PostgreSqlUri.class);
    }
}
