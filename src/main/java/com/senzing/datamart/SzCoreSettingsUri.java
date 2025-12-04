package com.senzing.datamart;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import com.senzing.util.JsonUtilities;

import static com.senzing.text.TextUtilities.urlDecodeUtf8;

/**
 * Represents a SQLite database connection URI.
 */
public class SzCoreSettingsUri extends ConnectionUri {
    /**
     * The scheme prefix for the URI's including the 
     * <code>"//core-settings/"</code> suffix.
     * The value of this is <code>{@value}</code>.
     */
    public static final String SCHEME_PREFIX = "sz://core-settings/";

    /**
     * Supported format for a Senzing Core Settings URI using a JSON path.
     * The value of this constant is <code>{value}</code>.
     */
    public static final String SUPPORTED_FORMAT
        = "sz://core-settings/[json-path]";

    /**
     * The <b>unmodifiable</b> {@link Set} of supported formats.
     * <ul>
     *  <li><code>{@value #SUPPORTED_FORMAT}</code></li>
     * </ul>
     */
    public static final Set<String> SUPPORTED_FORMATS 
        = Set.of(SUPPORTED_FORMAT);

    /**
     * The JSON path.
     */
    private String jsonPath = null;
    
    /**
     * The parsed JSON path.
     */
    private List<String> pathElements = null;

    /**
     * Constructs with the specified JSON path representing the
     * path to the JSON property in the Senzing Core SDK settings.
     * 
     * @param jsonPath The JSON path with each path element separated by 
     *                 a forward-slash.
     * 
     * @throws NullPointerException If the specified path is <code>null</code>.
     */
    public SzCoreSettingsUri(String jsonPath)
    {
        this(jsonPath, null);
    }

    /**
     * Constructs with the specified JSON path representing the
     * path to the JSON property in the Senzing Core SDK settings
     * and optional query options.
     * 
     * <p>
     * <b>NOTE:</b> If the first character of the specified path is 
     * not a forward slash, then the path will be prefixed with a forward
     * slash.
     * 
     * @param jsonPath The JSON path with each path element separated by 
     *                 a forward-slash.
     * 
     * @param queryOptions The optional {@link Map} of {@link String} query
     *                     parameter keys to {@link String} query parameter
     *                     values, or <code>null</code> if no parameters.
     * 
     * @throws NullPointerException If the specified path is <code>null</code>.
     * 
     * @throws IllegalArgumentException If there are any empty path elements
     *                                  in the specified path.
     */
    public SzCoreSettingsUri(String jsonPath, Map<String, String> queryOptions)
    {
        super(SCHEME_PREFIX, queryOptions);
        
        requireNonNull(jsonPath, "The JSON path cannot be null");

        // set the path ensuring the first character is a forward slash
        this.jsonPath = jsonPath.startsWith("/") 
            ? jsonPath : ("/" + jsonPath);

        String[] tokens = this.jsonPath.split("/");

        this.pathElements = new ArrayList<>(tokens.length);
        for (int index = 0; index < tokens.length; index++) {
            String token = tokens[index];
            if (index == 0 && token.length() == 0) {
                // skip the first empty token due to the leading slash
                continue;
            }

            // check for an empty path element
            if (token.length() == 0) {
                StringBuilder sb = new StringBuilder();
                for (String elem : this.pathElements) {
                    sb.append("/").append(elem);
                }
                throw new IllegalArgumentException(
                    "None of the path elements can be empty.  path=[ "
                    + jsonPath + " ], foundAfter=[ " + sb.toString() + " ]");
            }
            this.pathElements.add(urlDecodeUtf8(token));
        }

        // make the list unmodifiable
        this.pathElements = Collections.unmodifiableList(this.pathElements);
    }

    /**
     * Gets the JSON path for this instance.
     * 
     * @return The JSON path for this instance.
     */
    public String getPath() {
        return this.jsonPath;
    }

    /**
     * Gets the URL-decoded JSON path elements that were 
     * parsed from the {@linkplain #getPath() JSON path}
     * after splitting the path on <code>"/"</code>.
     * 
     * @return URL-decoded JSON path elements from the
     *         {@linkplain #getPath() JSON path}.  
     */
    public List<String> getPathElements() {
        return this.pathElements;
    }

    /**
     * Implemented to format the URI as a {@link String} with proper
     * URI encoding.
     * 
     * @return The formatted URI for this instance.
     */
    public String toString() {
        return SCHEME_PREFIX + this.getPath().substring(1)
             + this.getQueryString();
    }

    /**
     * Implemented to return a hash code consistent with the
     * {@link #equals(Object)} implementation.
     * 
     * @return The hash code for this instance.
     */
    public int hashCode() {
        return Objects.hash(this.getPath(),
                            this.getQueryOptions());
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
        SzCoreSettingsUri url = (SzCoreSettingsUri) obj;
        return Objects.equals(this.getPath(), url.getPath())
            && Objects.equals(this.getQueryOptions(), url.getQueryOptions());
    }

    /**
     * Provides a static factory method for parsing a 
     * Senzing Core Settings database URI for obtaining
     * the connection URI from the Senzing Core SDK settings.
     * 
     * @param uri The URI to parse.
     * 
     * @return The newly constructed {@link SzCoreSettingsUri} instance.
     * 
     * @throws NullPointerException If the specified parameter is
     *                              <code>null</code>.
     * 
     * @throws IllegalArgumentException If the specified URI is not properly
     *                                  formatted.
     */
    public static SzCoreSettingsUri parse(String uri) {
        requireNonNull(uri, "The URI cannot be null");

        // check the prefix
        if (!(uri.toLowerCase().startsWith(SCHEME_PREFIX) 
              && uri.length() >= SCHEME_PREFIX.length())) 
        {
            throw new IllegalArgumentException(
                "URI must begin with \"" + SCHEME_PREFIX + "\": " + uri);
        }

        // get the suffix (preserve the last / character of prefix)
        String suffix = uri.substring(SCHEME_PREFIX.length() - 1).trim();

        // get the path
        String path = null;

        // check for query options
        int index = suffix.indexOf('?');
        if (index < 0) {
            path = suffix;
            suffix = "";
        } else if (index == suffix.length() - 1) {
            path = suffix.substring(0, suffix.length() - 1);
        } else {
            path = suffix.substring(0, index);
            suffix = suffix.substring(index + 1);
        }
        
        // parse the query options
        Map<String, String> queryOptions = parseQueryOptions(suffix);
        
        // construct the instance
        return new SzCoreSettingsUri(path, queryOptions);
    }

    /**
     * Parses the specified JSON text as a {@link JsonObject} and returns
     * the result from {@link #resolveUri(JsonObject)}.
     * 
     * @param coreSettings The JSON text representing the 
     *                     Senzing Core SDK settings.
     * 
     * @return The resolved {@link ConnectionUri}, or <code>null</code>
     *         if the value is not found.
     * 
     * @throws NullPointerException If the specified parameter is <code>null</code>.
     * 
     * @throws IllegalArgumentException If a JSON path component is expected to
     *                                  be an array index, but is not.
     */
    public ConnectionUri resolveUri(String coreSettings) {
        Objects.requireNonNull(coreSettings, "Core settings cannot be null");
        JsonObject jsonObject = JsonUtilities.parseJsonObject(coreSettings);
        return this.resolveUri(jsonObject);
    }

    /**
     * Resolves the {@link ConnectionUri} for this instance using the
     * specified {@link JsonObject} representing the Senzing Core SDK
     * settings.
     * 
     * @param coreSettings The {@link JsonObject} representing the
     *                     Senzing Core SDK settings.
     * 
     * @return The resolved {@link ConnectionUri}, or <code>null</code>
     *         if the value is not found.
     * 
     * @throws NullPointerException If the specified parameter is <code>null</code>.
     * 
     * @throws IllegalArgumentException If a JSON path component is expected to
     *                                  be an array index, but is not.
     */
    public ConnectionUri resolveUri(JsonObject coreSettings) {
        Objects.requireNonNull(coreSettings, "Core settings cannot be null");
        JsonValue current = coreSettings;
        for (String property : this.getPathElements()) {
            if (current instanceof JsonObject) {
                JsonObject obj = (JsonObject) current;
                if (!obj.containsKey(property)) {
                    return null;
                }
                current = obj.get(property);
                
            } else if (current instanceof JsonArray) {
                JsonArray arr = (JsonArray) current;
                Integer index = null;
                try {
                    index = Integer.parseInt(property);
                    if (index < 0) {
                        throw new IllegalArgumentException(
                            "Array index cannot be negative: " + index);
                    }
                
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "Failed to parse path component (" + property
                        + ") from path (" + this.getPath() 
                        + ") as an array index: "
                        + JsonUtilities.toJsonText(current, true), e);
                }

                // if a valid index does not exist in the array, return null
                if (index >= arr.size()) {
                    return null;
                }
                current = arr.get(index);
            }
        }
        String uriText = null;
        if (current == null) {
            // value was resolved, but null was found
            return null;

        } else if (current instanceof JsonString) {
            uriText = ((JsonString) current).getString();
        } else {
            uriText = current.toString();
        }
        return ConnectionUri.parse(uriText);
    }

    static {
        registerConnectionType(SCHEME_PREFIX, SzCoreSettingsUri.class);
    }
}
