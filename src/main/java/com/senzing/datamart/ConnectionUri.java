package com.senzing.datamart;

import static com.senzing.text.TextUtilities.urlDecodeUtf8;
import static com.senzing.text.TextUtilities.urlEncodeUtf8;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base class for a URI that defines a connection to 
 * an external system such as a database or message queue.
 */
public abstract class ConnectionUri {
    /**
     * The scheme for the URI.
     */
    private String schemePrefix = null;

    /**
     * The query-string options for the URI.
     */
    private Map<String, String> queryOptions = null;

    /**
     * The {@link Map} that serves as a registry for scheme prefixes to 
     * {@link Method} instances representing the parse method for the URI
     * connection of that type.
     */
    private static final Map<String, Method> REGISTRY
        = new TreeMap<>((s1, s2) -> {
            // sort first by length
            int length1 = s1.length();
            int length2 = s2.length();
            int diff = length2 - length1;
            if (diff != 0) {
                return diff;
            }
            return s1.compareTo(s2);
        });

    /**
     * Registers a scheme prefix with an implementation of this class.  The
     * specified {@link Class} must have a method with the following signature:
     * <pre>
     *  public static T parse(String)
     * </pre>
     * Where <code>T</code> is the specified {@link Class}.
     * 
     * @param schemePrefix The scheme prefix to register.
     * @param urlClass The {@link ConnectionUri} implementation to register.
     * 
     * @throws NullPointerException If either parameter is <code>null</code>.
     * @throws IllegalStateException If specified scheme prefix is already registered.
     * @throws IllegalArgumentException If the specified {@link Class} does not have a
     *                                  parse method with a single {@link String} parameter
     *                                  and returns the specified {@link Class}.
     */
    protected static void registerConnectionType(
        String                          schemePrefix,
        Class<? extends ConnectionUri>  urlClass)
    {
        Objects.requireNonNull(schemePrefix, "Protocol prefix cannot be null");
        Objects.requireNonNull(urlClass, "The URI class cannot be null");
        String key = schemePrefix.toLowerCase();
        synchronized (REGISTRY) {
            if (REGISTRY.containsKey(key)) {
                throw new IllegalStateException(
                    "Protocol prefix (" + schemePrefix + ") already registered to "
                    + REGISTRY.get(key).getName() + ".");
            }

            // check for parse method
            Method method = null;
            try {
                method = urlClass.getMethod("parse", String.class);
            } catch (NoSuchMethodException e) {
                // ignore for now, we will trap below
            }

            int modifiers = (method == null) ? 0 : method.getModifiers();
            if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers)) {
                throw new IllegalArgumentException(
                    "The specified class does not have a public static " 
                    + urlClass.getName() + " parse(String) method: " + urlClass);
            }

            REGISTRY.put(key, method);
        }
    }

    /**
     * Constructs with the specified URI protocol.
     * 
     * @param schemePrefix The scheme prefix for database
     *                       connection URI's of this type.
     */
    protected ConnectionUri(String schemePrefix) {
        this(schemePrefix, null);
    }

    /**
     * Constructs with the specified scheme prefix and query options.
     * 
     * @param schemePrefix The scheme prefix for the URI.
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     */
    protected ConnectionUri(String schemePrefix, Map<String,String> queryOptions) {
        this.schemePrefix = schemePrefix;
        this.queryOptions   = (queryOptions == null) ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(queryOptions));
    }

    /**
     * Gets the scheme prefix for this instance.
     * 
     * @return The scheme prefix for this instance.
     */
    protected String getSchemePrefix() {
        return this.schemePrefix;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link Map} of query-string options
     * for this database connection URI.
     * 
     * @return The <b>unmodifiable</b> {@link Map} of query-string
     *         options for this database connection URI.
     */
    public Map<String, String> getQueryOptions() {
        return this.queryOptions;
    }

    /**
     * Parses the specified URI according to the registered
     * implementations of this class.
     * 
     * @param uri The URI to parse.
     * 
     * @return The {@link ConnectionUri} that was parsed.
     * 
     * @throws NullPointerException If the specified URI is <code>null</code>.
     * @throws IllegalArgumentException If the specified URI is not properly 
     *                                  formatted.
     */
    public static ConnectionUri parse(String uri) {
        Objects.requireNonNull(uri, "URI cannot be null");

        // find a method in the registry which is sorted by longest prefixes first
        Method[] methods = { null };
        synchronized (REGISTRY) {
            REGISTRY.forEach((prefix, m) -> {
                if (uri.toLowerCase().startsWith(prefix)) {
                    methods[0] = m;
                }
            });
        }

        // get the method and check if found
        Method method = methods[0];
        if (method == null) {
            throw new IllegalArgumentException(
                "Unrecognized URI pattern: " + uri);
        }
        
        // parse the URI
        try {
            return (ConnectionUri) method.invoke(null, uri);

        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                "Unexpected IllegalAccessException on public parse method: " 
                + method, e);

        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalArgumentException(
                "Unable to parse URI: " + uri, cause);
        }
    }

    /**
     * Parses the specified query string which may or may not begin with a 
     * <code>"?"</code> and returns a {@link Map} of {@link String} parameter
     * name keys to {@link String} values.
     * 
     * <p>
     * <b>NOTE:</b> While URI's allow for multi-valued parameters in a query
     * string (e.g.: <code>?foo=bar&amp;foo=bax</code>), this function only tracks
     * the <b>last</b> value encountered sine multi-valued query parameters
     * are uncommon or non-existent in connection URI's.
     * </p>
     * 
     * @param queryString The query string to parse.
     * 
     * @return The {@link Map} of {@link String} parameter names to
     *         {@link String} parameter values, or <code>null</code> if
     *         the specified parameter is <code>null</code> or contains
     *         no parameters.
     */
    protected static Map<String,String> parseQueryOptions(String queryString) {
        // check for a null or empty parameter
        if (queryString == null || queryString.trim().length() == 0) {
            return null;
        }

        // trim the string
        String suffix = queryString.trim();

        // check if its just a question mark
        if ("?".equals(suffix)) {
            return null;
        }

        // check if starts with question mark
        if (suffix.startsWith("?")) {
            // trim off the question mark
            suffix = suffix.substring(1);
        }

        // create the result
        Map<String,String> result = new LinkedHashMap<>();
        
        // check for an ampersand
        do {
            int index = suffix.indexOf('&');
            String pair = (index < 0) ? suffix : suffix.substring(0, index);
            suffix = (index >= 0 && index < suffix.length() -1) 
                ? suffix.substring(index + 1) : null;

            index = pair.indexOf('=');
            String key = (index < 0) ? pair : pair.substring(0, index);
            String value = (index < 0 || index == pair.length() - 1) 
                ? "" : pair.substring(index + 1);
            
            result.put(urlDecodeUtf8(key), urlDecodeUtf8(value));

        } while (suffix != null);

        // return the result
        return result;
    }

    /**
     * Gets the {@linkplain #getQueryOptions() query options} converted to a
     * URL-encoded query string with the <code>"?"</code> prefix.  This
     * returns empty string if there are no query options.
     * 
     * @return The encoded query string, or empty-string if no query options.
     */
    protected String getQueryString() {
        Map<String,String> map = this.getQueryOptions();
        if (map == null || map.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("?");
        map.forEach((key, value) -> {
            // check if this is not the first query option
            if (sb.length() > 1) {
                sb.append("&");
            }
            sb.append(urlEncodeUtf8(key));
            sb.append("=");
            sb.append(urlEncodeUtf8(value));
        });
        return sb.toString();
    }

    static {
        Class<ConnectionUri> c = ConnectionUri.class;
        String packageName = c.getPackageName();
        String[] classNames = {
            packageName + ".SqliteUri",
            packageName + ".PostgreSqlUri",
            packageName + ".RabbitMqUri",
            packageName + ".SqsUri",
            packageName + ".SzCoreSettingsUri"
        };
        for (String name : classNames) {
            try {
                // attempt to preload the class
                Class.forName(name);
            } catch (ClassNotFoundException ignore) {
                // ignore any exception
            }
        }
    }
}

