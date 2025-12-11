package com.senzing.datamart;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.senzing.text.TextUtilities.urlDecodeUtf8;
import static com.senzing.text.TextUtilities.urlEncodeUtf8;

/**
 * Represents a SQLite database connection URI.
 * 
 * <p>
 * <b>NOTE:</b> The Senzing environment initialization settings support Windows
 * file paths in the SQLite URI that contain back-slashes as file separators as
 * well as spaces that are not URL-encoded. While such URI's are not technically
 * legal, they may be encountered when working with Senzing environment
 * initialization settings on Windows, and therefore, this class treats them as
 * legal for parsing and attempts to reproduce the equivalent URI when formatted
 * via {@link #toString()} (save preserving the spaces without URL encoding).
 */
public class SQLiteUri extends ConnectionUri {
    /**
     * The query option key for the SQLite mode.
     */
    public static final String MODE_KEY = "mode";

    /**
     * The query option value for the {@link #MODE_KEY} that might be used to
     * indicate a memory-based connection.
     */
    public static final String MEMORY_MODE = "memory";

    /**
     * The scheme prefix for the URI's including the <code>":"</code>. The value of
     * this is <code>{@value}</code>.
     */
    public static final String SCHEME_PREFIX = "sqlite3:";

    /**
     * The special token used as a file path to indicate that the database is stored
     * in-memory rather than on the file system. The value of this is
     * <code>{@value}</code>.
     */
    public static final String MEMORY_TOKEN = ":memory:";

    /**
     * Supported format for a SQLite URI using a file path. An optional user and
     * password may be specified, but they are not used. The value of this constant
     * is <code>{value}</code>.
     */
    public static final String SUPPORTED_FORMAT_1 = "sqlite3://[unusedUser:unusedPassword@]/database_file_path";

    /**
     * Supported format for an in-memory SQLite URI using an fake file path as an
     * identifier. An optional user and password may be specified, but they are not
     * used. The value of this constant is <code>{value}</code>.
     */
    public static final String SUPPORTED_FORMAT_2 = "sqlite3://[unusedUser:unusedPassword@]/identifier?mode=memory&cache=shared";

    /**
     * Supported format for an in-memory SQLite URI. The value of this constant is
     * <code>{value}</code>.
     */
    public static final String SUPPORTED_FORMAT_3 = "sqlite3::memory:";

    /**
     * The <b>unmodifiable</b> {@link Set} of supported formats.
     * <ul>
     * <li><code>{@value #SUPPORTED_FORMAT_1}</code> (file path)</li>
     * <li><code>{@value #SUPPORTED_FORMAT_2}</code> (in-memory with path)</li>
     * <li><code>{@value #SUPPORTED_FORMAT_3}</code> (in-memory no path)</li>
     * </ul>
     */
    public static final Set<String> SUPPORTED_FORMATS = Collections
            .unmodifiableSet(Set.of(SUPPORTED_FORMAT_1, SUPPORTED_FORMAT_2, SUPPORTED_FORMAT_3));
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
     * The optional in-memory database identifier when the SQLite database is
     * in-memory, but a dummy file path was specified anyway.
     */
    private String inMemoryIdentifier = null;

    /**
     * Strips the query options {@link Map} of the {@link #MODE_KEY} key if its
     * value is {@link #MEMORY_MODE}.
     * 
     * @param queryOptions The query options, or <code>null</code> if none.
     * 
     * @return The modified query options {@link Map} of {@link String} keys to
     *         {@link String} values with the {@link #MODE_KEY} possibly removed.
     */
    private static Map<String, String> stripMemoryOption(Map<String, String> queryOptions) {
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
        Map<String, String> result = new LinkedHashMap<>();

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
     * Default constructor for a SQLite connection to an in-memory database.
     */
    public SQLiteUri() {
        this((Map<String, String>) null);
    }

    /**
     * Constructs an instance for a SQLite connection to an in-memory database with
     * the specified query options.
     * 
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter values,
     *                     or <code>null</code> if no parameters.
     * 
     * @throws IllegalArgumentException If the query options specify a
     *                                  {@linkplain #MODE_KEY mode} other than
     *                                  {@linkplain #MEMORY_MODE memory mode}.
     */
    public SQLiteUri(Map<String, String> queryOptions) {
        super(SCHEME_PREFIX, stripMemoryOption(queryOptions));
        if (queryOptions != null && queryOptions.containsKey(MODE_KEY)
                && !MEMORY_MODE.equalsIgnoreCase(queryOptions.get(MODE_KEY))) {
            throw new IllegalArgumentException("Can only specify " + MODE_KEY + "=" + MEMORY_MODE
                    + " query option if constructing without a path: " + queryOptions);
        }

        // nullify the other fields
        this.file = null;
        this.inMemoryIdentifier = null;
        this.unusedUser = null;
        this.unusedPassword = null;
    }

    /**
     * Constructs with the specified {@link File} representing the path to the
     * SQLite database file.
     * 
     * @param file The {@link File} for the path to the SQLite database.
     * 
     * @throws NullPointerException If the specified {@link File} is
     *                              <code>null</code>.
     */
    public SQLiteUri(File file) {
        this(file, null);
    }

    /**
     * Constructs with the specified {@link File} representing the path to the
     * SQLite database file and the specified {@link Map} of query options.
     * 
     * @param file         The {@link File} for the path to the SQLite database.
     * 
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter values,
     *                     or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If the specified {@link File} is
     *                              <code>null</code>.
     */
    public SQLiteUri(File file, Map<String, String> queryOptions) {
        this(null, null, file, queryOptions);
    }

    /**
     * Protected constructor to preserve unused fields.
     * 
     * @param unusedUser     A user to specify to include the URI even though the
     *                       user name is not used.
     * 
     * @param unusedPassword A password to specify to include the URI even though
     *                       the password is not used.
     * 
     * @param file           The {@link File} for the path to the SQLite database.
     * 
     * @param queryOptions   The optional {@link Map} of {@link String} query
     *                       parameter keys to {@link String} query parameter
     *                       values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If the specified {@link File} is
     *                              <code>null</code>.
     */
    protected SQLiteUri(String unusedUser, String unusedPassword, File file, Map<String, String> queryOptions) {
        super(SCHEME_PREFIX, queryOptions);

        requireNonNull(file, "The file cannot be null");
        if ((unusedUser == null && unusedPassword != null) || (unusedUser != null && unusedPassword == null)) {
            throw new IllegalArgumentException("Inconsistent user credentials.  Either both or neither "
                    + "should be null.  unusedUser=[ " + unusedUser + " ], unusedPassword=[ " + unusedPassword + " ]");
        }

        // check for memory mode
        String mode = (queryOptions != null) ? queryOptions.get(MODE_KEY) : null;

        // set the fields
        this.unusedUser = unusedUser;
        this.unusedPassword = unusedPassword;
        this.file = MEMORY_MODE.equals(mode) ? null : file;
        this.inMemoryIdentifier = MEMORY_MODE.equals(mode) ? file.toString() : null;
    }

    /**
     * Gets the {@link File} for this instance. This will return <code>null</code>
     * if this represents a SQLite connection to an in-memory database.
     * 
     * @return The {@link File} for this instance, or <code>null</code> if this
     *         represents a SQLite connection to an in-memory database.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Checks if this instance is using an in-memory database.
     * 
     * @return <code>true</code> if this instance is using an in-memory database,
     *         otherwise <code>false</code>.
     */
    public boolean isMemory() {
        return (this.file == null);
    }

    /**
     * Returns the user name that may have been specified even though the user name
     * is not used (e.g.: <code>"na"</code>).
     * 
     * @return The user name that may have been specified even though the user name
     *         is not used.
     */
    protected String getUnusedUser() {
        return this.unusedUser;
    }

    /**
     * Returns the password that may have been specified even though the password is
     * not used (e.g.: <code>"na"</code>).
     * 
     * @return The user name that may have been specified even though the user name
     *         is not used.
     */
    protected String getUnusedPassword() {
        return this.unusedPassword;
    }

    /**
     * Returns the identifier for the in-memory database that may have been
     * specified as a dummy file path when the database is designated as in-memory.
     * This allows for multiple distinct in-memory databases to exist.
     * 
     * @return The dummy file path that may have been specified as an in-memory
     *         database identifier.
     */
    public String getInMemoryIdentifier() {
        return this.inMemoryIdentifier;
    }

    /**
     * Implemented to format the URI as a {@link String} with proper URI encoding.
     * 
     * @return The formatted URI for this instance.
     */
    public String toString() {
        String unusedUser = this.getUnusedUser();
        String unusedPassword = this.getUnusedPassword();
        String memoryDbId = this.getInMemoryIdentifier();

        boolean unusedFields = (unusedUser != null || unusedPassword != null || memoryDbId != null);

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
            if (memoryDbId != null) {
                sb.append(memoryDbId);
            }
            sb.append(this.getQueryString());
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

            String path = this.file.getPath();
            if (isNonstandardWindowsPath(path)) {
                // handle windows drive letter and UNC paths since Senzing supports
                // backslashes in the paths even though URI's do not support this
                String drivePrefix = path.substring(0, 2);
                sb.append(drivePrefix);
                if (path.length() > 2) {
                    String pathSuffix = path.substring(2);
                    if (pathSuffix.equals("\\")) {
                        sb.append("\\");
                    } else {
                        String[] pathTokens = pathSuffix.split("\\\\");
                        String prefix = "";
                        for (String token : pathTokens) {
                            sb.append(prefix).append(urlEncodeUtf8(token));
                            prefix = "\\";
                        }
                    }
                }
            } else {
                // handle standard non-Windows paths
                String[] pathTokens = this.file.getPath().split("/");
                String prefix = "";
                for (String token : pathTokens) {
                    sb.append(prefix).append(urlEncodeUtf8(token));
                    prefix = "/";
                }
            }

            sb.append(this.getQueryString());
            return sb.toString();
        }
    }

    /**
     * Implemented to return a hash code consistent with the {@link #equals(Object)}
     * implementation.
     * 
     * @return The hash code for this instance.
     */
    public int hashCode() {
        return Objects.hash(this.getFile(), this.isMemory(), this.getUnusedUser(), this.getUnusedPassword(),
                this.getInMemoryIdentifier(), this.getQueryOptions());
    }

    /**
     * Implemented to return <code>true</code> if and only if the specified
     * parameter is a non-null reference to an object of the same class with
     * equivalent properties.
     * 
     * @param obj The object to compare with.
     * 
     * @return <code>true</code> if and only if the objects are equal, otherwise
     *         <code>false</code>.
     */
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SQLiteUri url = (SQLiteUri) obj;
        return Objects.equals(this.isMemory(), url.isMemory()) && Objects.equals(this.getFile(), url.getFile())
                && Objects.equals(this.getUnusedUser(), url.getUnusedUser())
                && Objects.equals(this.getUnusedPassword(), url.getUnusedPassword())
                && Objects.equals(this.getInMemoryIdentifier(), url.getInMemoryIdentifier())
                && Objects.equals(this.getQueryOptions(), url.getQueryOptions());
    }

    /**
     * Provides a static factory method for parsing a SQLite database connection URI
     * formatted as a {@link String}.
     * 
     * <p>
     * <b>NOTE:</b> The Senzing environment initialization settings support Windows
     * file paths in the SQLite URI that contain back-slashes as file separators as
     * well as spaces that are not URL-encoded. While such URI's are not technically
     * legal, they may be encountered when working with Senzing environment
     * initialization settings on Windows, and therefore, this function treats them
     * as legal for parsing.
     * 
     * @param uri The URI to parse.
     * 
     * @return The newly constructed {@link SQLiteUri} instance.
     * 
     * @throws NullPointerException     If the specified parameter is
     *                                  <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified URI is not properly
     *                                  formatted.
     */
    public static SQLiteUri parse(String uri) {
        requireNonNull(uri, "The URI cannot be null");

        // check the prefix
        if (!(uri.toLowerCase().startsWith(SCHEME_PREFIX) && uri.length() > SCHEME_PREFIX.length())) {
            throw new IllegalArgumentException("URI must begin with \"" + SCHEME_PREFIX + "\": " + uri);
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
            return new SQLiteUri(parseQueryOptions(suffix));
        } else if (suffix.startsWith("//") && suffix.length() > 2) {
            suffix = suffix.substring(2);
        } else {
            throw new IllegalArgumentException("Unrecognized URI format: " + uri);
        }

        // find the unused user if any
        index = suffix.indexOf('@');

        String unusedUser = null;
        String unusedPassword = null;
        if (index >= 0) {
            String authority = suffix.substring(0, index);
            suffix = (index < suffix.length() - 1) ? suffix.substring(index + 1) : "";
            index = authority.indexOf(':');
            if (index < 0) {
                // treat the whole authority as a user with no password
                unusedUser = urlDecodeUtf8(authority);

            } else {
                // split on the ":" and get the user and password
                unusedUser = urlDecodeUtf8(authority.substring(0, index));

                unusedPassword = (index < authority.length() - 1) ? authority.substring(index + 1) : "";
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
        Map<String, String> map = parseQueryOptions(suffix);
        boolean memory = (map != null) && MEMORY_MODE.equals(map.get(MODE_KEY));

        if (!memory && path.length() == 0) {
            throw new IllegalArgumentException("Unable to parse path from URI: " + uri);
        }

        // build a file object
        if (isNonstandardWindowsPath(path)) {
            // do a simple conversion for the path for non-standard paths
            file = new File(urlDecodeUtf8(path));
        } else {
            // actually parse the path as a URI and check it is valid
            try {
                file = new File(new URI("file://" + path));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Error parsing path (" + path + ") from URI: " + uri, e);
            }
        }

        // construct the instance
        return new SQLiteUri(unusedUser, unusedPassword, file, map);
    }

    /**
     * Checks if the specified path appears to be a Windows path that is not legally
     * formatted for a standard URI.
     * 
     * @param path The path to check.
     * 
     * @return <code>true</code> if a non-standard Windows path is encountered,
     *         otherwise <code>false</code>.
     */
    private static boolean isNonstandardWindowsPath(String path) {
        // ensure the path is trimmed of all whitespace
        path = path.trim();

        // path length is less than 2, so it cannot begin with
        // a drive letter prefix, nor can it start with double-backslash (UNC)
        if (path.length() < 2) {
            return false;
        }

        // path length is exactly 2, this could be a bare drive letter prefix
        // with no slash, but CANNOT be a UNC paths
        if (path.length() == 2 && path.charAt(1) == ':') {
            return startsWithLegalWindowsDriveLetter(path);
        }

        // check for the drive letter prefix
        if (path.substring(1).startsWith(":\\")) {
            return startsWithLegalWindowsDriveLetter(path);
        }

        // finally check for a UNC path which should begin with two backslash
        // characters followed by a non-backslash character
        return (path.startsWith("\\\\") && path.charAt(2) != '\\');
    }

    /**
     * Checks if the first character of a string is a potential Windows drive
     * letter.
     * 
     * @param path The path to check.
     * 
     * @return <code>true</code> if the specified {@link String} starts with a
     *         potential Windows drive letter, otherwise <code>false</code>
     */
    private static boolean startsWithLegalWindowsDriveLetter(String path) {
        String upperPrefix = path.substring(0, 1).toUpperCase();
        char driveLetter = upperPrefix.charAt(0);
        return (driveLetter >= 'A' && driveLetter <= 'Z');
    }

    static {
        registerConnectionType(SCHEME_PREFIX, SQLiteUri.class);
    }
}
