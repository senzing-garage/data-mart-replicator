package com.senzing.datamart;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.cmdline.ParameterProcessor;
import com.senzing.util.JsonUtilities;

import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.io.IOUtilities.readTextFileAsString;
import static com.senzing.util.LoggingUtilities.formatStackTrace;
import static com.senzing.util.LoggingUtilities.multilineFormat;

/**
 * The startup options for the data mart replicator.
 */
@SuppressWarnings("rawtypes")
public enum SzReplicatorOption implements CommandLineOption<SzReplicatorOption, SzReplicatorOption> {
    /**
     * <p>
     * Option for displaying help/usage for the replicator. This option can only be
     * provided by itself and has no parameters.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--help</code></li>
     * </ul>
     */
    HELP("--help", null, null, true, 0),

    /**
     * <p>
     * Option for displaying the version number of the replicator. This option can
     * only be provided by itself and has no parameters.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--version</code></li>
     * </ul>
     */
    VERSION("--version", null, null, true, 0),

    /**
     * <p>
     * Option for ignoring environment variables when setting the values for other
     * command-line options. A single parameter may optionally be specified as
     * <code>true</code> or <code>false</code> with <code>false</code> simulating
     * the absence of the option.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--ignore-environment [true|false]</code></li>
     * </ul>
     */
    IGNORE_ENVIRONMENT("--ignore-environment", null, null, 0, "false"),

    /**
     * <p>
     * Option for specifying the module name to initialize the Senzing API's with.
     * The default value is {@link SzReplicatorConstants#DEFAULT_INSTANCE_NAME}.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--instance-name {module-name}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_INSTANCE_NAME="{module-name}"</code></li>
     * </ul>
     */
    CORE_INSTANCE_NAME("--core-instance-name", ENV_PREFIX + "CORE_INSTANCE_NAME", null, 1, DEFAULT_INSTANCE_NAME),

    /**
     * <p>
     * Option for specifying the core settings JSON with which to initialize the
     * Core Senzing SDK. The parameter to this option should be the settings as a
     * JSON object <b>or</b> the path to a file containing the settings JSON.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--core-settings [{file-path}|{json-text}]</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_SETTINGS="[{file-path}|{json-text}]"</code></li>
     * </ul>
     */
    CORE_SETTINGS("--core-settings", ENV_PREFIX + "CORE_SETTINGS", List.of("SENZING_ENGINE_CONFIGURATION_JSON"), true,
            1),

    /**
     * <p>
     * This option is used with {@link #CORE_SETTINGS} to force a specific
     * configuration ID to be used for initialization and prevent automatic
     * reinitialization to pickup the latest default config ID.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--config-id {config-id}</code></li>
     * <li>Environment: <code>SENZING_TOOLS_CORE_CONFIG_ID="{config-id}"</code></li>
     * </ul>
     */
    CORE_CONFIG_ID("--core-config-id", ENV_PREFIX + "CORE_CONFIG_ID", null, 1),

    /**
     * <p>
     * This presence of this option determines if the Core Senzing SDK is
     * initialized in verbose mode. The default value if not specified is
     * <code>muted</code> (which is equivalent to zero). The parameter to this
     * option may be specified as one of:
     * <ul>
     * <li><code>muted</code> - To indicate no logging.</li>
     * <li><code>verbose</code> - To indicate verbose logging.</li>
     * <li><code>0</code> - To indicate no logging.</li>
     * <li><code>1</code> - To indicate verbose logging.</li>
     * </ul>
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line:
     * <code>--core-log-level [muted|verbose|{integer}]</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_LOG_LEVEL="[muted|verbose|{integer}]"</code></li>
     * </ul>
     */
    CORE_LOG_LEVEL("--core-log-level", ENV_PREFIX + "CORE_LOG_LEVEL", null, 0, "muted"),

    /**
     * <p>
     * This option sets the number of threads available for executing Core Senzing
     * SDK functions. The single parameter to this option should be a positive
     * integer. If not specified, then this defaults to
     * {@link SzReplicatorConstants#DEFAULT_CORE_CONCURRENCY},
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--core-concurrency {thread-count}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_CONCURRENCY="{thread-count}"</code></li>
     * </ul>
     */
    CORE_CONCURRENCY("--core-concurrency", ENV_PREFIX + "CORE_CONCURRENCY", null, 1, DEFAULT_CORE_CONCURRENCY_PARAM),

    /**
     * <p>
     * If leveraging the default configuration stored in the database, this option
     * is used to specify how often the gRPC server should background check that the
     * current active config is the same as the current default config and update
     * the active config if not. The parameter to this option is specified as an
     * integer:
     * <ul>
     * <li>A positive integer is interpreted as a number of seconds.</li>
     * <li>If zero is specified, the auto-refresh is disabled and it will only occur
     * when a requested configuration element is not found in the current active
     * config.</li>
     * <li>Specifying a negative integer is allowed but is used to enable a check
     * and conditional refresh only when manually requested (programmatically).</li>
     * </ul>
     * <b>NOTE:</b> This is option ignored if auto-refresh is disabled because the
     * config was specified via the <code>G2CONFIGFILE</code> in the
     * {@link #CORE_SETTINGS} or if {@link #CORE_CONFIG_ID} has been specified to
     * lock in a specific configuration.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--refresh-config-seconds {integer}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_REFRESH_CONFIG_SECONDS="{integer}"</code></li>
     * </ul>
     */
    REFRESH_CONFIG_SECONDS("--refresh-config-seconds", ENV_PREFIX + "REFRESH_CONFIG_SECONDS", null, 1,
            DEFAULT_REFRESH_CONFIG_SECONDS_PARAM),

    /**
     * <p>
     * Use this option to balance the message consumption and processing between
     * aggressively keeping the data mart closely in sync with the entity repository
     * and less frequent batch processing to conserve system resources. The value to
     * this option is one of the following:
     * <ul>
     * <li><code>leisurely</code> -- This setting allows for longer gaps between
     * updating the data mart, favoring less frequent batch processing in order to
     * conserve system resources.</li>
     * 
     * <li><code>standard</code> -- This is the default and is balance between
     * conserving system resources and keeping the data mart updated in a reasonably
     * timely manner.</li>
     * 
     * <li><code>aggressive</code> -- This setting uses more system resources to
     * aggressively consume and process incoming messages to keep the data mart
     * closely in sync with the least time delay.</li>
     * 
     * </ul>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line:
     * <code>--processing-rate {leisurely|standard|aggressive}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_REFRESH_CONFIG_SECONDS="{integer}"</code></li>
     * </ul>
     */
    PROCESSING_RATE("--processing-rate", ENV_PREFIX + "PROCESSING_RATE", null, 1,
            ProcessingRate.STANDARD.toString().toLowerCase()),

    /**
     * <p>
     * This option is used to specify the URL to an Amazon SQS queue to be used for
     * obtaining the info messages. The single parameter to this option is the URL.
     * If this option is specified then the info queue parameters for RabbitMQ are
     * not allowed.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--sqs-info-uri {url}</code></li>
     * <li>Environment: <code>SENZING_SQS_INFO_URI="{url}"</code></li>
     * </ul>
     */
    SQS_INFO_URI("--sqs-info-uri", ENV_PREFIX + "SQS_INFO_QUEUE_URL", null, 1),

    /**
     * <p>
     * This option is used to specify the URL to the RabbitMQ server for finding the
     * RabbitMQ info queue. The single parameter to this option is an AMQP URL. If
     * this option is specified then the SQS info queue parameter is not allowed.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line:
     * <code>--rabbit-info-uri amqp://user:password@host:port/vhost</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_RABBITMQ_URI="amqp://user:password@host:port/vhost"</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_RABBITMQ_URI="amqp://user:password@host:port/vhost" (fallback)</code></li>
     * </ul>
     */
    RABBITMQ_URI("--rabbit-info-uri", ENV_PREFIX + "RABBITMQ_URI", List.of(ENV_PREFIX + "RABBITMQ_URI"), 1),

    /**
     * <p>
     * This option is used to specify the routing key for connecting to RabbitMQ as
     * part of specifying a RabbitMQ info queue. The single parameter to this option
     * is a routing key. If this option is specified then the other options required
     * for a RabbitMQ info queue are required and the info queue parameters
     * pertaining to SQS and Kafka are not allowed.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--rabbit-info-queue {queue-name}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_RABBITMQ_INFO_QUEUE="{queue-name}"</code></li>
     * </ul>
     */
    RABBITMQ_INFO_QUEUE("--rabbit-info-queue", ENV_PREFIX + "RABBITMQ_INFO_QUEUE", null, 1),

    /**
     * <p>
     * This presence of this option causes the data mart replicator to utilize a
     * database table message queue instead of Rabbit MQ or Amazon SQS. The data
     * mart replicator will use the same database that is configured for the data
     * mart to find the <code>sz_message_queue</code> table from which to consume
     * messages. The absence of this parameter will causes the data mart to require
     * additional options for configuring the message queue for Rabbit MQ or Amazon
     * SQS. A single parameter may optionally be specified as <code>true</code> or
     * <code>false</code> with <code>false</code> simulating the absence of the
     * option.
     * <p>
     * <b>NOTE:</b> If using SQLite then only a single database connection from a
     * single process is allowed at any one time, and therefore either a process
     * embedding the data mart must be populating the queue concurrently or
     * population of the queue by another process must occur while the data mart
     * replicator is <b>not</b> consuming the messages from the
     * <code>sz_message_queue</code> table.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--database-info-queue [true|false]</code></li>
     * <li>Environment:
     * <code>SENZING_DATA_MART_DATABASE_INFO_QUEUE="{true|false}"</code></li>
     * </ul>
     */
    DATABASE_INFO_QUEUE("--database-info-queue", ENV_PREFIX + "DATABASE_INFO_QUEUE", null, 0, "false"),

    /**
     * This option is used to specify the database connection for the data mart. The
     * single parameter to this option is the SQLite or PostgreSQL database URL
     * specifying the database connection. Possible database URL formats are:
     * <ul>
     * <li><code>{@value PostgreSqlUri#SUPPORTED_FORMAT_1}</code></li>
     * <li><code>{@value PostgreSqlUri#SUPPORTED_FORMAT_2}</code></li>
     * <li><code>{@value SQLiteUri#SUPPORTED_FORMAT_1}</code></li>
     * <li><code>{@value SQLiteUri#SUPPORTED_FORMAT_2}</code></li>
     * <li><code>{@value SQLiteUri#SUPPORTED_FORMAT_3}</code></li>
     * </ul>
     * <b>NOTE:</b> The PostgreSQL or SQLite URI can also be obtained from the
     * {@link #CORE_SETTINGS} by using a special URI in the following format:
     * <ul>
     * <li><code>{@value SzCoreSettingsUri#SUPPORTED_FORMAT}</code></li>
     * </ul>
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--database-uri {url}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_DATA_MART_DATABASE_URI="{url}"</code></li>
     * </ul>
     * <p>
     * The default value for this option if not specified is
     * {@link SzReplicatorConstants#DEFAULT_CORE_SETTINGS_DATABASE_URI}. This is so
     * it attempts to obtain the database URI from the {@linkplain #CORE_SETTINGS
     * Senzing Core SDK settings}.
     */
    DATABASE_URI("--database-uri", ENV_PREFIX + "DATA_MART_DATABASE_URI", null, 1, DEFAULT_CORE_SETTINGS_DATABASE_URI);

    /**
     * Constructs with the specified parameters.
     *
     * @param cmdLineFlag    The command-line flag.
     * @param envVariable    The primary environment variable.
     * @param envFallbacks   The {@link List} of fallback environment variables.
     * @param parameterCount The number of parameters for the option.
     */
    SzReplicatorOption(String cmdLineFlag, String envVariable, List<String> envFallbacks, int parameterCount) {
        this(cmdLineFlag, envVariable, envFallbacks, false, parameterCount);
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param cmdLineFlag       The command-line flag.
     * @param envVariable       The primary environment variable.
     * @param envFallbacks      The {@link List} of fallback environment variables.
     * @param parameterCount    The number of parameters for the option.
     * @param defaultParameters The default parameter values for the option if not
     *                          specified.
     */
    SzReplicatorOption(String cmdLineFlag, String envVariable, List<String> envFallbacks, int parameterCount, String... defaultParameters) {
        this(cmdLineFlag, envVariable, envFallbacks, false, parameterCount, defaultParameters);
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param primary           <code>true</code> if this is a primary option,
     *                          otherwise <code>false</code>.
     * @param cmdLineFlag       The command-line flag.
     * @param envVariable       The primary environment variable.
     * @param envFallbacks      The {@link List} of fallback environment variables.
     * @param parameterCount    The number of parameters for the option.
     * @param defaultParameters The default parameter value for the option if not
     *                          specified.
     */
    SzReplicatorOption(String cmdLineFlag, String envVariable, List<String> envFallbacks, boolean primary, int parameterCount, String... defaultParameters) {
        this.primary = primary;
        this.cmdLineFlag = cmdLineFlag;
        this.envVariable = envVariable;
        this.minParamCount = (parameterCount < 0) ? 0 : parameterCount;
        this.maxParamCount = parameterCount;
        this.envFallbacks = (envFallbacks == null) ? null : List.copyOf(envFallbacks);
        this.defaultParameters = (defaultParameters == null) ? Collections.emptyList() : List.of(defaultParameters);
    }

    /**
     * Whether or not the option is a primary option.
     */
    private boolean primary = false;

    /**
     * The command-line flag for the option.
     */
    private String cmdLineFlag = null;

    /**
     * The environment variable for the option.
     */
    private String envVariable = null;

    /**
     * The fallback environment variables for the option in descending priority
     * order.
     */
    private List<String> envFallbacks = null;

    /**
     * The minimum number of expected parameters.
     */
    private int minParamCount = 0;

    /**
     * The maximum number of expected parameters.
     */
    private int maxParamCount = -1;

    /**
     * The default parameter values for the option.
     */
    private List<String> defaultParameters = null;

    /**
     * The {@link Map} of option keys to values that are sets of dependency sets.
     */
    private static final Map<SzReplicatorOption, Set<Set<CommandLineOption>>> DEPENDENCIES;

    /**
     * The {@link Map} of option keys to values that are sets of conflicting
     * options.
     */
    private static final Map<SzReplicatorOption, Set<CommandLineOption>> CONFLICTING_OPTIONS;

    /**
     * The {@link Map} of {@link String} keys mapping command-line flags to
     * {@link SzReplicatorOption} values.
     */
    private static final Map<String, SzReplicatorOption> OPTIONS_BY_FLAG;

    @Override
    public String getCommandLineFlag() {
        return this.cmdLineFlag;
    }

    @Override
    public int getMinimumParameterCount() {
        return this.minParamCount;
    }

    @Override
    public int getMaximumParameterCount() {
        return this.maxParamCount;
    }

    @Override
    public List<String> getDefaultParameters() {
        return this.defaultParameters;
    }

    @Override
    public boolean isPrimary() {
        return this.primary;
    }

    @Override
    public String getEnvironmentVariable() {
        return this.envVariable;
    }

    @Override
    public List<String> getEnvironmentFallbacks() {
        return this.envFallbacks;
    }

    @Override
    public Set<CommandLineOption> getConflicts() {
        return CONFLICTING_OPTIONS.get(this);
    }

    @Override
    public Set<Set<CommandLineOption>> getDependencies() {
        return DEPENDENCIES.get(this);
    }

    @Override
    public boolean isSensitive() {
        switch (this) {
        case RABBITMQ_URI:
        case DATABASE_URI:
        case CORE_SETTINGS:
            return true;
        default:
            return false;
        }
    }

    static {
        // force load the URI classes
        Class<?>[] classes = { ConnectionUri.class, SQLiteUri.class, PostgreSqlUri.class, RabbitMqUri.class,
                SQSUri.class, SzCoreSettingsUri.class };
        for (Class c : classes) {
            try {
                // attempt to preload the class
                Class.forName(c.getName());
            } catch (ClassNotFoundException ignore) {
                // ignore any exception
            }
        }

        Map<SzReplicatorOption, Set<Set<CommandLineOption>>> dependencyMap = new LinkedHashMap<>();
        Map<SzReplicatorOption, Set<CommandLineOption>> conflictMap = new LinkedHashMap<>();
        Map<String, SzReplicatorOption> lookupMap = new LinkedHashMap<>();

        try {
            // iterate over the options
            for (SzReplicatorOption option : SzReplicatorOption.values()) {
                conflictMap.put(option, new LinkedHashSet<>());
                dependencyMap.put(option, new LinkedHashSet<>());
                lookupMap.put(option.getCommandLineFlag().toLowerCase(), option);
            }

            SzReplicatorOption[] exclusiveOptions = { HELP, VERSION };
            for (SzReplicatorOption option : SzReplicatorOption.values()) {
                for (SzReplicatorOption exclOption : exclusiveOptions) {
                    if (option == exclOption) {
                        continue;
                    }
                    Set<CommandLineOption> set = conflictMap.get(exclOption);
                    set.add(option);
                    set = conflictMap.get(option);
                    set.add(exclOption);
                }
            }

            // handle the messaging options
            Set<SzReplicatorOption> rabbitInfoOptions = Set.of(RABBITMQ_URI, RABBITMQ_INFO_QUEUE);

            Set<CommandLineOption> requiredRabbit = Set.of(RABBITMQ_URI, RABBITMQ_INFO_QUEUE);

            Set<CommandLineOption> sqsInfoOptions = Set.of(SQS_INFO_URI);

            Set<CommandLineOption> dbInfoOptions = Set.of(DATABASE_INFO_QUEUE);

            // enforce that we only have one info queue
            for (SzReplicatorOption option : rabbitInfoOptions) {
                Set<CommandLineOption> conflictSet = conflictMap.get(option);
                conflictSet.addAll(sqsInfoOptions);
                conflictSet.addAll(dbInfoOptions);
            }
            for (CommandLineOption option : sqsInfoOptions) {
                Set<CommandLineOption> conflictSet = conflictMap.get(option);
                conflictSet.addAll(rabbitInfoOptions);
                conflictSet.addAll(dbInfoOptions);
            }
            for (CommandLineOption option : dbInfoOptions) {
                Set<CommandLineOption> conflictSet = conflictMap.get(option);
                conflictSet.addAll(sqsInfoOptions);
                conflictSet.addAll(rabbitInfoOptions);
            }

            // make the optional rabbit options dependent on the required ones
            for (SzReplicatorOption option : rabbitInfoOptions) {
                if (requiredRabbit.contains(option)) {
                    continue;
                }
                Set<Set<CommandLineOption>> dependencySets = dependencyMap.get(option);
                dependencySets.add(requiredRabbit);
            }

            List<Set<CommandLineOption>> baseDependSets = new LinkedList<>();
            Set<CommandLineOption> dependSet = new LinkedHashSet<>();
            dependSet.add(SQS_INFO_URI);
            dependSet.add(DATABASE_URI);
            baseDependSets.add(Collections.unmodifiableSet(dependSet));

            dependSet = new LinkedHashSet<>();
            dependSet.add(DATABASE_URI);
            dependSet.addAll(requiredRabbit);
            baseDependSets.add(Collections.unmodifiableSet(dependSet));

            SzReplicatorOption[] initOptions = { CORE_SETTINGS };
            // make the primary options dependent on one set of info queue options
            for (SzReplicatorOption option : initOptions) {
                Set<Set<CommandLineOption>> dependencySets = dependencyMap.get(option);

                dependencySets.addAll(baseDependSets);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println(formatStackTrace(e.getStackTrace()));
            throw new ExceptionInInitializerError(e);

        } finally {
            DEPENDENCIES = Collections.unmodifiableMap(dependencyMap);
            CONFLICTING_OPTIONS = Collections.unmodifiableMap(conflictMap);
            OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);
        }
    }

    /**
     * The {@link ParameterProcessor} implementation for this class.
     */
    private static class ParamProcessor implements ParameterProcessor {
        /**
         * Processes the parameters for the specified option.
         *
         * @param option The {@link SzReplicatorOption} to process.
         * @param params The {@link List} of parameters for the option.
         * @return The processed value.
         * @throws IllegalArgumentException If the specified {@link CommandLineOption}
         *                                  is not an instance of
         *                                  {@link SzReplicatorOption} or is otherwise
         *                                  unrecognized.
         */
        public Object process(CommandLineOption option, List<String> params) {
            if (!(option instanceof SzReplicatorOption)) {
                throw new IllegalArgumentException(
                        "Unhandled command line option: " + option.getCommandLineFlag() + " / " + option);
            }

            // down-cast
            SzReplicatorOption replicatorOption = (SzReplicatorOption) option;
            switch (replicatorOption) {
            case HELP:
            case VERSION:
                return Boolean.TRUE;

            case IGNORE_ENVIRONMENT:
            case DATABASE_INFO_QUEUE:
                if (params.size() == 0) {
                    return Boolean.TRUE;
                }
                String boolText = params.get(0);
                if ("false".equalsIgnoreCase(boolText)) {
                    return Boolean.FALSE;
                }
                if ("true".equalsIgnoreCase(boolText)) {
                    return Boolean.TRUE;
                }
                throw new IllegalArgumentException("The specified parameter for " + option.getCommandLineFlag()
                        + " must be true or false: " + params.get(0));

            case CORE_INSTANCE_NAME:
                return params.get(0).trim();

            case CORE_SETTINGS: {
                String paramVal = params.get(0).trim();
                if (paramVal.length() == 0) {
                    throw new IllegalArgumentException("Missing parameter for core settings.");
                }
                if (paramVal.startsWith("{")) {
                    try {
                        return JsonUtilities.parseJsonObject(paramVal);

                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                multilineFormat("Core settings is not valid JSON: ", paramVal));
                    }
                } else {
                    File initFile = new File(paramVal);
                    if (!initFile.exists()) {
                        throw new IllegalArgumentException("Specified JSON init file does not exist: " + initFile);
                    }
                    String jsonText;
                    try {
                        jsonText = readTextFileAsString(initFile, "UTF-8");

                    } catch (IOException e) {
                        throw new RuntimeException(
                                multilineFormat("Failed to read JSON initialization file: " + initFile, "",
                                        "Cause: " + e.getMessage()));
                    }
                    try {
                        return JsonUtilities.parseJsonObject(jsonText);

                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                "The initialization file does not contain valid JSON: " + initFile);
                    }
                }
            }
            case CORE_CONFIG_ID:
                try {
                    return Long.parseLong(params.get(0));
                } catch (Exception e) {
                    throw new IllegalArgumentException("The configuration ID for " + option.getCommandLineFlag()
                            + " must be an integer: " + params.get(0));
                }

            case CORE_LOG_LEVEL: {
                String paramVal = params.get(0).trim().toLowerCase();

                switch (paramVal) {
                case "verbose":
                case "1":
                    return true;
                case "muted":
                case "0":
                    return false;
                default:
                    throw new IllegalArgumentException("The specified core log level is not recognized; " + paramVal);
                }
            }

            case CORE_CONCURRENCY: {
                int threadCount;
                try {
                    threadCount = Integer.parseInt(params.get(0));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Thread count must be an integer: " + params.get(0));
                }
                if (threadCount <= 0) {
                    throw new IllegalArgumentException("Negative thread counts are not allowed: " + threadCount);
                }
                return threadCount;
            }

            case REFRESH_CONFIG_SECONDS:
                try {
                    return Long.parseLong(params.get(0));
                } catch (Exception e) {
                    throw new IllegalArgumentException("The specified refresh period for " + option.getCommandLineFlag()
                            + " must be an integer: " + params.get(0));
                }

            case PROCESSING_RATE:
                return parseProcessingRate(params.get(0));

            case SQS_INFO_URI:
                return SQSUri.parse(params.get(0));

            case RABBITMQ_URI:
                return RabbitMqUri.parse(params.get(0));

            case RABBITMQ_INFO_QUEUE:
                return params.get(0);

            case DATABASE_URI:
                return parseDatabaseUri(params.get(0));

            default:
                throw new IllegalArgumentException(
                        "Unhandled command line option: " + option.getCommandLineFlag() + " / " + option);

            }
        }
    }

    /**
     * Parses the specified parameter value as a database {@link ConnectionUri}.
     * 
     * @param paramValue The parameter value to parse.
     * 
     * @return The {@link ConnectionUri} that was parsed.
     */
    public static ProcessingRate parseProcessingRate(String paramValue) {
        Objects.requireNonNull(paramValue, "Parameter value cannot be null");
        try {
            return ProcessingRate.valueOf(paramValue.trim().toUpperCase());

        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            String prefix = "";
            for (ProcessingRate value : ProcessingRate.values()) {
                sb.append(prefix).append(value.toString().toLowerCase());
                prefix = ", ";
            }
            throw new IllegalArgumentException(
                    "Unrecognized processing rate value (" + paramValue + ").  Should be one of: " + sb.toString());
        }
    }

    /**
     * Parses the specified parameter value as a database {@link ConnectionUri}.
     * 
     * @param paramValue The parameter value to parse.
     * 
     * @return The {@link ConnectionUri} that was parsed.
     */
    public static ConnectionUri parseDatabaseUri(String paramValue) {
        Objects.requireNonNull(paramValue, "Parameter value cannot be null");
<<<<<<< HEAD
        Set<Class<? extends ConnectionUri>> allowed = Set.of(PostgreSqlUri.class, SqliteUri.class);

=======
        Set<Class<? extends ConnectionUri>> allowed
            = Set.of(PostgreSqlUri.class, SQLiteUri.class);
        
>>>>>>> main
        ConnectionUri uri = ConnectionUri.parse(paramValue);
        if (!allowed.contains(uri.getClass())) {
            throw new IllegalArgumentException("Unrecognized database connection URI: " + paramValue);
        }
        return uri;
    }

    /**
     * The {@link ParameterProcessor} for {@link SzReplicatorOption}. This instance
     * will only handle instances of {@link CommandLineOption} instances of type
     * {@link SzReplicatorOption}.
     */
    public static final ParameterProcessor PARAMETER_PROCESSOR = new ParamProcessor();

}
