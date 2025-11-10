package com.senzing.datamart;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.senzing.text.TextUtilities.urlDecodeUtf8;
import static com.senzing.text.TextUtilities.urlEncodeUtf8;

/**
 * Represents a SQLite database connection URI.
 */
public class SqliteUri extends ConnectionUri {
    /**
     * The query option key for the SQLite mode.
     */
    public static final String MODE_KEY = "mode";

    /**
     * The query option value for the {@link #MODE_KEY} that might
     * be used to indicate a memory-based connection.
     */
    public static final String MEMORY_MODE = "memory";

    /**
     * The scheme prefix for the URI's including the <code>":"</code>.
     * The value of this is <code>{@value}</code>.
     */
    public static final String SCHEME_PREFIX = "sqlite3:";

    /**
     * The special token used as a file path to indicate that the
     * database is stored in-memory rather than on the file system.
     * The value of this is <code>{@value}</code>.
     */
    public static final String MEMORY_TOKEN = ":memory:";

    /**
     * The {@link File} for the SQLite database.
     */
    private File file = null;

    /**
     * The unused user name if one was specified (typically <code>"na"</code>).
     */
    private String unusedUser = null;

    /**
     * The unused password if one was specified (typically <code>"na"</code>).
     */
    private String unusedPassword = null;

    /**
     * The unused {@link File} when the SQLite database is in-memory,
     * but a {@link File} was specified anyway.
     */
    private File unusedFile = null;

    /**
     * Strips the query options {@link Map} of the {@link #MODE_KEY} key if 
     * its value is {@link #MEMORY_MODE}.
     * 
     * @param queryOptions The query options, or <code>null</code> if none.
     * 
     * @return The modified query options {@link Map} of {@link String} keys to 
     *         {@link String} values with the {@link #MODE_KEY} possibly removed.
     */
    private static Map<String,String> stripMemoryOption(Map<String,String> queryOptions)
    {
        if (queryOptions == null) {
            return null;
        }

        // get the mode
        String mode = queryOptions.get(MODE_KEY);

        // check if the mode is memory
        if (!MEMORY_MODE.equals(mode)) {
            return queryOptions;
        }

        // create the result map
        Map<String,String> result = new LinkedHashMap<>();

        // add every other query option except the mode
        queryOptions.forEach((key, value) -> {
            // we stripped the memory mode
            if (!MODE_KEY.equalsIgnoreCase(key)) {
                result.put(key, value);
            }
        });

        // return the result
        return result;
    }

    /**
     * Default constructor for a SQLite connection to an 
     * in-memory database.
     */
    public SqliteUri() {
        this((Map<String,String>) null);
    }

    /**
     * Constructs an instance for a SQLite connection to an
     * in-memory database with the specified query options.
     * 
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     */
    public SqliteUri(Map<String,String> queryOptions) {
        super(SCHEME_PREFIX, queryOptions);
    }

    /**
     * Constructs with the specified {@link File} representing the
     * path to the SQLite database file.
     * 
     * @param file The {@link File} for the path to the SQLite database.
     * 
     * @throws NullPointerException If the specified {@link File}
     *                              is <code>null</code>.
     */
    public SqliteUri(File file)
    {
        this(file, null);
    }

    /**
     * Constructs with the specified {@link File} representing the
     * path to the SQLite database file and the specified {@link Map}
     * of query options.
     * 
     * @param file The {@link File} for the path to the SQLite database.
     * 
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If the specified {@link File}
     *                              is <code>null</code>.
     */
    public SqliteUri(File file, Map<String,String> queryOptions)
    {
        this(null, null, file, queryOptions);
    }

    /**
     * Protected constructor to preserve unused fields.
     * 
     * @param file The {@link File} for the path to the SQLite database.
     * 
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If the specified {@link File}
     *                              is <code>null</code>.
     */
    protected SqliteUri(String              unusedUser,
                        String              unusedPassword, 
                        File                file, 
                        Map<String,String>  queryOptions)
    {
        super(SCHEME_PREFIX, stripMemoryOption(queryOptions));
        
        requireNonNull(file, "The file cannot be null");

        // check for memory mode
        String mode = queryOptions.get(MODE_KEY);

        // set the fields
        this.file = MEMORY_MODE.equals(mode) ? null : file;
        this.unusedFile = MEMORY_MODE.equals(mode) ? file : null;
    }

    /**
     * Gets the {@link File} for this instance.  This will return 
     * <code>null</code> if this represents a SQLite connection to
     * an in-memory database.
     * 
     * @return The {@link File} for this instance, or <code>null</code>
     *         if this represents a SQLite connection to an in-memory
     *         database.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Checks if this instance is using an in-memory database.
     * 
     * @return <code>true</code> if this instance is using an in-memory
     *         database, otherwise <code>false</code>.
     */
    public boolean isMemory() {
        return (this.file == null);
    }

    /**
     * Returns the user name that may have been specified even though
     * the user name is not used (e.g.: <code>"na"</code>).
     * 
     * @return The user name that may have been specified even though
     *         the user name is not used.
     */
    protected String getUnusedUser() {
        return this.unusedUser;
    }

    /**
     * Returns the password that may have been specified even though
     * the password is not used (e.g.: <code>"na"</code>).
     * 
     * @return The user name that may have been specified even though
     *         the user name is not used.
     */
    protected String getUnusedPassword() {
        return this.unusedPassword;
    }

    /**
     * Returns the file that may have been specified even though the
     * database is designated as in-memory and the file is not used.
     * 
     * @return The file that may have been specified in association 
     *         with an in-memory SQLite database connection.
     */
    protected File getUnusedFile() {
        return this.unusedFile;
    }

    /**
     * Implemented to format the URI as a {@link String} with proper
     * URI encoding.
     * 
     * @return The formatted URI for this instance.
     */
    public String toString() {
        String unusedUser       = this.getUnusedUser();
        String unusedPassword   = this.getUnusedPassword();
        File   unusedFile       = this.getUnusedFile();

        boolean unusedFields
            = (unusedUser != null || unusedPassword != null || unusedFile != null);

        if (this.isMemory() && !unusedFields) {
            return SCHEME_PREFIX + MEMORY_TOKEN + this.getQueryString();

        } else if (this.isMemory()) {
            StringBuilder sb = new StringBuilder();
            sb.append(SCHEME_PREFIX).append("//");
            if (unusedUser != null) {
                sb.append(unusedUser);
                if (unusedPassword != null) {
                    sb.append(":").append(unusedPassword);
                }
                sb.append("@");
            }
            if (unusedFile != null) {
                sb.append(unusedFile.getPath());
            }
            sb.append("?").append(MODE_KEY).append("=").append(MEMORY_MODE);
            String queryString = this.getQueryString();
            if (queryString != null && queryString.trim().length() > 0) {
                sb.append("&").append(queryString.trim().substring(1));
            }
            return sb.toString();

        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(SCHEME_PREFIX).append("//");
            if (unusedUser != null) {
                sb.append(unusedUser);
                if (unusedPassword != null) {
                    sb.append(":").append(unusedPassword);
                }
                sb.append("@");
            }
            String[] pathTokens = this.file.getPath().split("/");
            String prefix = "";
            for (String token : pathTokens) {
                sb.append(prefix).append(urlEncodeUtf8(token));
                prefix = "/";
            }
            sb.append(this.getQueryString());
            return sb.toString();
        }
    }

    /**
     * Implemented to return a hash code consistent with the
     * {@link #equals(Object)} implementation.
     * 
     * @return The hash code for this instance.
     */
    public int hashCode() {
        return Objects.hash(this.getFile(),
                            this.isMemory(),
                            this.getUnusedUser(),
                            this.getUnusedPassword(),
                            this.getUnusedFile(),
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
        SqliteUri url = (SqliteUri) obj;
        return Objects.equals(this.isMemory(), url.isMemory())
            && Objects.equals(this.getFile(), url.getFile())
            && Objects.equals(this.getUnusedUser(), url.getUnusedUser())
            && Objects.equals(this.getUnusedPassword(), url.getUnusedPassword())
            && Objects.equals(this.getUnusedFile(), url.getUnusedFile())
            && Objects.equals(this.getQueryOptions(), url.getQueryOptions());
    }

    /**
     * Provides a static factory method for parsing a PostgreSQL database
     * connection URI formatted as a {@link String}.
     * 
     * @param uri The URI to parse.
     * 
     * @return The newly constructed {@link SqliteUri} instance.
     * 
     * @throws NullPointerException If the specified parameter is
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified URI is not properly
     *                                  formatted.
     */
    public static SqliteUri parse(String uri) {
        requireNonNull(uri, "The URI cannot be null");

        // check the prefix
        if (!(uri.toLowerCase().startsWith(SCHEME_PREFIX) 
              && uri.length() > SCHEME_PREFIX.length())) 
        {
            throw new IllegalArgumentException(
                "URI must begin with \"" + SCHEME_PREFIX + "\": " + uri);
        }

        // get the suffix
        String suffix = uri.substring(SCHEME_PREFIX.length()).trim();

        // get the file
        File file = null;
        int index = 0;

        // check for memory token
        if (suffix.startsWith(MEMORY_TOKEN)) {
            if (suffix.length() > MEMORY_TOKEN.length()) {
                suffix = suffix.substring(MEMORY_TOKEN.length()).trim();
            } else {
                suffix = "";
            }
            return new SqliteUri(parseQueryOptions(suffix));
        }

        // find the unused user if any
        index = suffix.indexOf('@');

        String unusedUser = null;
        String unusedPassword = null;
        if (index >= 0) {
            String authority = suffix.substring(index);
            suffix = (index < suffix.length() - 1) ? suffix.substring(index + 1) : "";
            index = authority.indexOf(':');
            if (index < 0) {
                // treat the whole authority as a user with no password
                unusedUser = urlDecodeUtf8(authority);

            } else {
                // split on the ":" and get the user and password
                unusedUser = urlDecodeUtf8(authority.substring(0, index));

                unusedPassword = (index < authority.length() - 1)
                    ? authority.substring(index + 1) : "";
                unusedPassword = urlDecodeUtf8(unusedPassword);
            }
        }

        // find the query string if any
        index = suffix.indexOf('?');
        String path = (index < 0) ? suffix : suffix.substring(0, index);

        if (suffix.length() > path.length() + 1) {
            suffix = suffix.substring(path.length());
        } else {
            suffix = "";
        }

        // parse the query options
        Map<String,String> map = parseQueryOptions(suffix);
        boolean memory = (map != null) && MEMORY_MODE.equals(map.get(MODE_KEY));
        
        if (!memory && path.length() == 0) {
            throw new IllegalArgumentException(
                "Unable to parse path from URI: " + uri);
        }        

        // build a file object
        try {
            file = new File(new URI("file://" + path));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                "Error parsing path (" + path + ") from URI: " + uri, e);
        }
        
        // construct the instance
        return new SqliteUri(unusedUser, unusedPassword, file, map);
    }

    static {
        registerConnectionType(SCHEME_PREFIX, SqliteUri.class);
    }
}
