package com.senzing.datamart;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.datamart.SzReplicatorOption.*;
import static com.senzing.util.JsonUtilities.*;

/**
 * Describes the options to be set when constructing an instance of
 * {@link SzReplicator}.
 */
public class SzReplicatorOptions {
    /**
     * Used to annotate methods with their associated {@link SzReplicatorOption}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Option {
        /**
         * Gets the {@link SzReplicatorOption} associated with the method that it
         * annotates.
         * 
         * @return The {@link SzReplicatorOption} associated with the method that it
         *         annotates.
         */
        SzReplicatorOption value();
    }

    /**
     * The {@link Map} of {@link SzReplicatorOption} keys to {@link Method}
     * values for getting the associated value.
     */
    private static final Map<SzReplicatorOption, Method> GETTER_METHODS;

    /**
     * The {@link Map} of {@link SzReplicatorOption} keys to {@link Method}
     * values for setting the associated value.
     */
    private static final Map<SzReplicatorOption, Method> SETTER_METHODS;

    /**
     * The {@link Map} of {@link String} property name keys to 
     * {@link Method} values for getting the associated value.
     */
    private static final Map<String, Method> GETTERS_BY_NAME;

    /**
     * The {@link Map} of {@link String} property name keys to 
     * {@link Method} values for setting the associated value.
     */
    private static final Map<String, Method> SETTERS_BY_NAME;

    /**
     * The instance name with which to initialize the code SDK.
     */
    private String coreInstanceName = DEFAULT_INSTANCE_NAME;

    /**
     * The settings with which to initialize the code SDK.
     */
    private JsonObject coreSettings = null;

    /**
     * The config ID with which to initialize the core SDK (or null).
     */
    private Long coreConfigId = null;

    /**
     * The log level with which to initialize the core SDK.
     */
    private int coreLogLevel = 0;

    /**
     * The concurrency with which to initialize the auto core SDK.
     */
    private int coreConcurrency = DEFAULT_CORE_CONCURRENCY;

    /**
     * The database URL for connecting to the database.
     */
    private ConnectionUri databaseUri = null;

    /**
     * The SQS info URI, or <code>null</code> if using alternate
     * message queue.
     */
    private SqsUri sqsInfoUri = null;

    /**
     * Whether or not to use the info queue in the database.
     */
    private boolean useDatabaseQueue = false;

    /**
     * The {@link RabbitMqUri} for the RabbitMQ connection.
     */
    private RabbitMqUri rabbitMqUri = null;

    /**
     * The name of the RabbitMQ queue to read info messages from.
     */
    private String rabbitInfoQueue = null;

    /**
     * The config refresh period (in seconds) with which to initialize
     * the auto core SDK.
     */
    private long refreshConfigSeconds = DEFAULT_REFRESH_CONFIG_SECONDS;
    
    /**
     * Constructs with the {@link Map} of {@link CommandLineOption}
     * keys to {@link Object} values.
     * 
     * @param optionMap The {@link Map} of {@link CommandLineOption}
     *                  keys to {@link Object} values.
     */
    @SuppressWarnings("rawtypes")
    protected SzReplicatorOptions(Map<CommandLineOption, Object> optionsMap)
    {
        setOptions(this, optionsMap);
    }   

    /**
     * Default constructor.
     */
    public SzReplicatorOptions() {
        // do nothing
    }

    /**
     * Constructs with the native Senzing JSON initialization parameters as a
     * {@link JsonObject}.
     *
     * @param jsonInit The JSON initialization parameters.
     */
    public SzReplicatorOptions(JsonObject settings) {
        Objects.requireNonNull(
            settings, "JSON init parameters cannot be null");
        this.coreSettings = settings;
    }

    /**
     * Constructs with the native Senzing JSON initialization parameters as JSON
     * text.
     *
     * @param jsonInitText The JSON initialization parameters as JSON text.
     */
    public SzReplicatorOptions(String settings) {
        this(JsonUtilities.parseJsonObject(settings));
    }

    /**
     * Returns the {@link JsonObject} describing the Senzing SDK
     * initialization settings.
     *
     * @return The {@link JsonObject} describing the Senzing SDK
     *         initialization settings.
     */
    @Option(CORE_SETTINGS)
    public JsonObject getCoreSettings() {
        return this.coreSettings;
    }

    /**
     * Returns the {@link JsonObject} describing the Senzing SDK
     * initialization settings.
     *
     * @return The {@link JsonObject} describing the Senzing SDK
     *         initialization settings.
     */
    @Option(CORE_SETTINGS)
    public SzReplicatorOptions setCoreSettings(JsonObject settings) {
        this.coreSettings = settings;
        return this;
    }

    /**
     * Gets the number of threads that the server will create for the
     * Senzing Core SDK operations.  If the value has not {@linkplain
     * #setCoreConcurrency(Integer) explicitly set} then {@link 
     * SzReplicatorConstants#DEFAULT_CORE_CONCURRENCY} is returned.
     *
     * @return The number of threads that the server will create for
     *         Senzing Core SDK operations.
     */
    @Option(CORE_CONCURRENCY)
    public int getCoreConcurrency() {
        return this.coreConcurrency;
    }

    /**
     * Sets the number of threads that the server will create for the
     * Senzing Core SDK operations.  Set to <code>null</code> to use the {@linkplain
     * SzReplicatorConstants#DEFAULT_CORE_CONCURRENCY default number of threads}.
     *
     * @param concurrency The number of threads to create for Senzing Core SDK
     *                    operations, or <code>null</code> for the default number
     *                    of threads.
     *
     * @return A reference to this instance.
     */
    @Option(CORE_CONCURRENCY)
    public SzReplicatorOptions setCoreConcurrency(Integer concurrency) {
        this.coreConcurrency = (concurrency != null)
                ? concurrency
                : DEFAULT_CORE_CONCURRENCY;
        return this;
    }

    /**
     * Gets the instance name with which to initialize the core Senzing SDK
     * via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#instanceName(String)}.
     * If <code>null</code> is returned then {@link 
     * SzReplicatorConstants#DEFAULT_INSTANCE_NAME} is used.
     *
     * @return The instance name with which to initialize the core Senzing SDK,
     *         or <code>null</code> if {@link 
     *         SzReplicatorConstants#DEFAULT_INSTANCE_NAME} should be used.
     */
    @Option(CORE_INSTANCE_NAME)
    public String getCoreInstanceName() {
        return this.coreInstanceName;
    }

    /**
     * Sets the instance name with which to initialize the core Senzing SDK
     * via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#instanceName(String)}.
     * Set to <code>null</code> if the default value of {@link 
     * SzReplicatorConstants#DEFAULT_INSTANCE_NAME} is to be used.
     *
     * @param instanceName The instance name with which to initialize the core
     *                   Senzing SDK, or <code>null</code> then the
     *                   {@link SzReplicatorConstants#DEFAULT_INSTANCE_NAME}
     *                   should be used. 
     * 
     * @return A reference to this instance.
     */
    @Option(CORE_INSTANCE_NAME)
    public SzReplicatorOptions setCoreInstanceName(String instanceName) {
        this.coreInstanceName = instanceName;
        return this;
    }

    /**
     * Gets the log level with which to initialize the core Senzing SDK.
     * This returns an integer, which currently translates into a boolean
     * for {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#verboseLogging(boolean)}
     * that is <code>true</code> for non-zero values and <code>false</code>
     * for zero (0).  If the verbosity has not been {@linkplain 
     * #setCoreLogLevel(int) explicitly set} then <code>false</code> is
     * returned.
     *
     * @return Gets the log level to determine how to set the verbosity for
     *         the core Senzing SDK.
     */
    @Option(CORE_LOG_LEVEL)
    public int getCoreLogLevel() {
        return this.coreLogLevel;
    }

    /**
     * Sets the log level with which to initialize the core Senzing SDK.
     * This is set as an integer, which currently translates into a boolean
     * for {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#verboseLogging(boolean)}
     * that is <code>true</code> for non-zero values and <code>false</code>
     * for zero (0).
     *
     * @param logLevel The log level to determine how to set the verbosity
     *                 for the core Senzing SDK.
     *
     * @return A reference to this instance.
     */
    @Option(CORE_LOG_LEVEL)
    public SzReplicatorOptions setCoreLogLevel(int logLevel) {
        this.coreLogLevel = logLevel;
        return this;
    }

    /**
     * Gets the explicit configuration ID with which to initialize the core
     * Senzing SDK via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#configId(Long)}
     * This method returns <code>null</code> if the data mart should use
     * the current default configuration ID from the repository.  This method
     * returns <code>null</code> if the value has not been {@linkplain 
     * #setCoreConfigurationId(Long) explicitly set}.
     *
     * @return The explicit configuration ID with which to initialize the
     *         Senzing native engine API, or <code>null</code> if the data
     *         mart should use the current default configuration ID from
     *         the repository.
     */
    @Option(CORE_CONFIG_ID)
    public Long getCoreConfigurationId() {
        return this.coreConfigId;
    }

    /**
     * Sets the explicit configuration ID with which to initialize the core
     * Senzing SDK via {@link com.senzing.sdk.core.SzCoreEnvironment.Builder#configId(Long)}.
     * Set the value to <code>null</code> if the data mart should use the 
     * current default configuration ID from the entity repository.
     *
     * @param configId The explicit configuration ID with which to initialize
     *                 the core Senzing SDK, or <code>null</code> if the data
     *                 mart should use the current default configuration ID
     *                 from the repository.
     *
     * @return A reference to this instance.
     */
    @Option(CORE_CONFIG_ID)
    public SzReplicatorOptions setCoreConfigurationId(Long configId) {
        this.coreConfigId = configId;
        return this;
    }

    /**
     * Returns the auto refresh period which is positive to indicate a number
     * of seconds to delay, zero if configuration refresh should only occur
     * reactively (not periodically), and a negative number to indicate that
     * configuration refresh should be disabled.
     *
     * @return The auto refresh period.
     */
    @Option(REFRESH_CONFIG_SECONDS)
    public long getRefreshConfigSeconds() {
        return this.refreshConfigSeconds;
    }

    /**
     * Sets the configuration auto refresh period. Set the value to
     * <code>null</code> if the API server should use {@link
     * SzReplicatorConstants#DEFAULT_REFRESH_CONFIG_SECONDS}.
     * Use zero (0) to indicate that the configuration should only
     * be refreshed in reaction to detecting it is out of sync 
     * after a failure and a negative integer to disable configuration
     * refresh entirely.
     *
     * @param seconds The number of seconds between periodic automatic
     *                refresh of the configuration, zero (0) to only 
     *                refresh reactively, and a negative integer to
     *                never refresh.
     *
     * @return A reference to this instance.
     */
    @Option(REFRESH_CONFIG_SECONDS)
    public SzReplicatorOptions setRefreshConfigSeconds(Long seconds) {
        this.refreshConfigSeconds = (seconds == null)
            ? DEFAULT_REFRESH_CONFIG_SECONDS : seconds;
        return this;
    }

    /**
     * Checks the configured database should be used to consume messages via the
     * <code>sz_message_queue</code> table instead of using Rabbit MQ or Amazon SQS.
     * 
     * @return <code>true</code> if the configured database should be used for the
     *         info message queue, otherwise <code>false</code>
     */
    @Option(DATABASE_INFO_QUEUE)
    public boolean isUsingDatabaseQueue() {
        return this.useDatabaseQueue;
    }

    /**
     * Sets the whether or not the configured database should be used to provide the
     * info message queue via the <code>sz_message_queue</code> table rather than
     * using Rabbit MQ or Amazon SQS.
     *
     * @param useDatabaseQueue <code>true</code> if the configured database should
     *                         be used for the info message queue, otherwise
     *                         <code>false</code>.
     */
    @Option(DATABASE_INFO_QUEUE)
    public void setUsingDatabaseQueue(boolean useDatabaseQueue) {
        if (useDatabaseQueue) {
            if (this.getSqsInfoUri() != null) {
                throw new IllegalStateException(
                    "Cannot use a database info queue if "
                    + "using an SQS info queue.");
            }
            if (this.getRabbitMqUri() != null 
                || this.getRabbitMqInfoQueue() != null) 
            {
                throw new IllegalStateException(
                    "Cannot use a database info queue if "
                    + "using a RabbitMQ info queue.");
            }
        }

        this.useDatabaseQueue = useDatabaseQueue;
    }

    /**
     * Gets the RabbitMqUri for the RabbitMQ connection.
     * 
     * @return The RabbitMqUri for the RabbitMQ connection, or
     *         <code>null</code> if not using RabbitMQ.
     */
    @Option(RABBITMQ_URI)
    public RabbitMqUri getRabbitMqUri() {
        return this.rabbitMqUri;
    }

    /**
     * Sets the RabbitMqUri for the RabbitMQ connection for the
     * the "info" queue.
     *
     * @param uri The RabbitMqUri for the RabbitMQ connection.
     *
     * @return A reference to this instance.
     */
    @Option(RABBITMQ_URI)
    public SzReplicatorOptions setRabbitMqUri(RabbitMqUri uri) {
        if (uri != null) {
            if (this.isUsingDatabaseQueue()) {
                throw new IllegalStateException(
                    "Cannot specify a RabbitMQ URI if using "
                    + "a database info queue");
            }
            if (this.getSqsInfoUri() != null) {
                throw new IllegalStateException(
                    "Cannot specify a RabbitMQ URI if using "
                    + "an SQS info queue.");
            }
        }
        this.rabbitMqUri = uri;
        return this;
    }

    /**
     * Returns the RabbitMQ queue name for the "info" queue.
     *
     * @return The RabbitMQ routing key for the "info" queue.
     */
    @Option(RABBITMQ_INFO_QUEUE)
    public String getRabbitMqInfoQueue() {
        return this.rabbitInfoQueue;
    }

    /**
     * Sets the RabbitMQ queue name for the "info" queue.
     *
     * @param queueName The RabbitMQ queue name for the "info" queue.
     *
     * @return A reference to this instance.
     */
    @Option(RABBITMQ_INFO_QUEUE)
    public SzReplicatorOptions setRabbitMqInfoQueue(String queueName) {
        if (queueName != null) {
            if (this.isUsingDatabaseQueue()) {
                throw new IllegalStateException(
                    "Cannot specify a RabbitMQ queue if using "
                    + "a database info queue");
            }
            if (this.getSqsInfoUri() != null) {
                throw new IllegalStateException(
                    "Cannot specify a RabbitMQ queue if using "
                    + "an SQS info queue.");
            }
        }
        this.rabbitInfoQueue = queueName;
        return this;
    }

    /**
     * Returns the {@link SqsUri} for the "info" queue.
     *
     * @return The {@link SqsUri} for the "info" queue.
     */
    @Option(SQS_INFO_URI)
    public SqsUri getSqsInfoUri() {
        return this.sqsInfoUri;
    }

    /**
     * Sets the {@link SqsUri} for the "info" queue.
     *
     * @param uri The {@link SqsUri} for the "info" queue.
     *
     * @return A reference to this instance.
     */
    @Option(SQS_INFO_URI)
    public SzReplicatorOptions setSqsInfoUri(SqsUri uri) {
        if (uri != null) {
            if (this.isUsingDatabaseQueue()) {
                throw new IllegalStateException(
                    "Cannot specify an SQS URI if using "
                    + "a database info queue");
            }
            if (this.getRabbitMqUri() != null 
                || this.getRabbitMqInfoQueue() != null) 
            {
                throw new IllegalStateException(
                    "Cannot specify an SQS URI if using "
                    + "a RabbitMQ info queue.");
            }
        }
        this.sqsInfoUri = uri;
        return this;
    }

    /**
     * Gets the database {@link ConnectionUri} for the 
     * data mart database.
     *
     * @return The database {@link ConnectionUri} for the 
     *         data mart database.
     *
     * @see SzReplicatorOption#DATABASE_URI
     */
    @Option(DATABASE_URI)
    public ConnectionUri getDatabaseUri() {
        return this.databaseUri;
    }

    /**
     * Sets the database {@link ConnectionUri} for the
     * data mart database.
     *
     * @param uri The {@link ConnectionUri} for connecting to
     *            the data mart database.
     *
     * @see SzReplicatorOption#DATABASE_URI
     */
    @Option(DATABASE_URI)
    public SzReplicatorOptions setDatabaseUri(ConnectionUri uri) 
    {
        if ((uri != null) && (!(uri instanceof PostgreSqlUri))
            && (!(uri instanceof SqliteUri))) 
        {
            throw new IllegalArgumentException(
                "Unsupported database connection URL type: " + uri);
        }

        this.databaseUri = uri;
        return this;
    }

    /**
     * Converts this instance to a {@link JsonObject}.
     *
     * @return A {@link JsonObject} representation of this instance.
     */
    public JsonObject toJson() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        this.buildJson(builder);
        return builder.build();
    }

    /**
     * Gets a property name from a method name by stripping the 
     * the first lower-case portion and then converting the first
     * letter to lower case thereafter.
     * 
     * @param method The method from which to get the property name.
     * 
     * @return The property name.
     */
    private static String getPropertyName(Method method) {
        String name = method.getName();
        int firstUpper = 0;
        for (int index = 0; index < name.length(); index++) {
            if (Character.isUpperCase(name.charAt(index))) {
                firstUpper = index;
                break;
            }
        }
        return name.substring(firstUpper, firstUpper + 1).toLowerCase()
            + ((firstUpper < name.length() - 1) ? name.substring(firstUpper + 1) : "");
    }

    /**
     * Converts this instance to JSON.
     *
     * @param builder The {@link JsonObjectBuilder} to which to add the JSON
     *                properties, or <code>null</code> if a new
     *                {@link JsonObjectBuilder} should be created.
     * @return The specified {@link JsonObjectBuilder}.
     */
    public JsonObjectBuilder buildJson(JsonObjectBuilder builder) {
        JsonObjectBuilder job = (builder == null)
            ? Json.createObjectBuilder() : builder;

        GETTERS_BY_NAME.forEach((propertyName, getter) -> {
            Object value = null;
            try {
                value = getter.invoke(this);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);

            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(e);
            }

            // add the value if not null
            if (value != null) {
                // add the value to the builder with the property name
                JsonUtilities.addProperty(job, propertyName, value);
            }
        });

        // return the builder
        return job;
    }

    /**
     * Parses the specified JSON text as an instance of {@link SzReplicatorOptions}.
     *
     * @param jsonText The JSON text describing the {@link SzReplicatorOptions}.
     *
     * @return The {@link SzReplicatorOptions} created from the specified JSON text.
     */
    public static SzReplicatorOptions parse(String jsonText) {
        JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);
        return parse(jsonObject);
    }

    /**
     * Parses the specified {@link JsonObject} as an instance of
     * {@link SzReplicatorOptions}.
     *
     * @param jsonObject The {@link JsonObject} describing the
     *                   {@link SzReplicatorOptions}.
     *
     * @return The {@link SzReplicatorOptions} created from the specified
     *         {@link JsonObject}.
     */
    public static SzReplicatorOptions parse(JsonObject jsonObject) 
    {
        final JsonObject obj = jsonObject;
        SzReplicatorOptions opts = new SzReplicatorOptions();

        SETTERS_BY_NAME.forEach((propertyName, setter) -> {
            // skip any property that is missing a value
            if (!obj.containsKey(propertyName)) {
                return;
            }

            // get the parameter type
            Class<?> paramType = setter.getParameterTypes()[0];
            Object value = getValue(paramType, obj, propertyName);
            try {
                setter.invoke(opts, value);

            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);

            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(e);
            }
        });
        return opts;
    }

    /**
     * Creates a {@link Map} of {@link CommandLineOption} keys to {@link Object}
     * values for initializing an {@link SzGrpcServer} instance.
     *
     * @return The {@link Map} of {@link CommandLineOption} keys to {@link Object}
     *         values for initializing an {@link SzGrpcServer} instance
     */
    @SuppressWarnings("rawtypes")
    protected Map<CommandLineOption, Object> buildOptionsMap() 
    {
        Map<CommandLineOption, Object> map = new HashMap<>();
        GETTER_METHODS.forEach((option, method) -> {
            try {
                put(map, option, method.invoke(this));

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);

            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            }
        });
        return map;
    }

    /**
     * Sets the options on the specified instance using the
     * specified {@link Map} of options to values.
     * 
     * @param options The {@link SzReplicatorOptions} on which to
     *                set the option values.
     * @param optionsMap The {@link Map} of {@link SzReplicatorOptions}
     *                   keys to {@link Object} values.
     */
    @SuppressWarnings("rawtypes")
    protected static void setOptions(
        SzReplicatorOptions             options,
        Map<CommandLineOption, Object>  optionsMap) 
    {
        optionsMap.forEach((option, value) -> {
            Method method = SETTER_METHODS.get(option);
            if (method != null) {
                try {
                    method.invoke(options, value);

                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);

                } catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                }
            }
        });
    }

    /**
     * Utility method to only put non-null values in the specified {@link Map} with
     * the specified {@link SzReplicatorOption} key and {@link Object} value.
     *
     * @param map    The {@link Map} to put the key-value pair into.
     * @param option The {@link SzReplicatorOption} key.
     * @param value  The {@link Object} value.
     */
    @SuppressWarnings("rawtypes")
    private static void put(Map<CommandLineOption, Object>  map,
                            SzReplicatorOption              option,
                            Object                          value) 
    {
        if (value != null) {
            map.put(option, value);
        }
    }

    static {
        Map<SzReplicatorOption, Method> getterMap = new LinkedHashMap<>();
        Map<SzReplicatorOption, Method> setterMap = new LinkedHashMap<>();

        Class<SzReplicatorOptions> cls = SzReplicatorOptions.class;
        Method[] methods = cls.getMethods();
        for (Method method : methods) {
            Option option = method.getAnnotation(Option.class);
            if (option == null) {
                continue;
            }
            // check if the setter or getter
            if ((method.getReturnType() == SzReplicatorOptions.class)
                && (method.getParameterTypes().length == 1))
            {
                setterMap.put(option.value(), method);

            } else if ((method.getReturnType() != Void.class)
                && (method.getParameterTypes().length == 0))
            {
                getterMap.put(option.value(), method);
            }
        }

        GETTER_METHODS = Collections.unmodifiableMap(getterMap);
        SETTER_METHODS = Collections.unmodifiableMap(setterMap);

        Map<String, Method> getByName = new TreeMap<>();
        Map<String, Method> setByName = new TreeMap<>();
        getterMap.values().forEach(method -> {
            String propertyName = getPropertyName(method);
            getByName.put(propertyName, method);
        });
        setterMap.values().forEach(method -> {
            String propertyName = getPropertyName(method);
            setByName.put(propertyName, method);
        });
        if (!getByName.keySet().equals(setByName.keySet())) {
            Set<String> missingGetters = new TreeSet<>(getByName.keySet());
            Set<String> missingSetters = new TreeSet<>(setByName.keySet());
            missingGetters.removeAll(setByName.keySet());
            missingSetters.removeAll(getByName.keySet());

            throw new IllegalStateException(
                "Setters and getter methods are not consistent.  missingGetters=[ "
                + missingGetters + " ], missingSetters=[ " + missingSetters + " ]");
        }
        GETTERS_BY_NAME = Collections.unmodifiableMap(getByName);
        SETTERS_BY_NAME = Collections.unmodifiableMap(setByName);
    }

}
