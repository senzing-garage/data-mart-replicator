package com.senzing.datamart;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.cmdline.ParameterProcessor;
import com.senzing.util.JsonUtilities;

import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.io.IOUtilities.readTextFileAsString;
import static com.senzing.util.LoggingUtilities.multilineFormat;

/**
 * The startup options for the data mart replicator.
 */
public enum SzReplicatorOption implements CommandLineOption {
  /**
   * <p>
   * Option for displaying help/usage for the replicator.  This option can
   * only be provided by itself and has no parameters.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--help</code></li>
   * </ul>
   */
  HELP("--help", null, null, true, 0),

  /**
   * <p>
   * Option for displaying the version number of the replicator.  This option
   * can only be provided by itself and has no parameters.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--version</code></li>
   * </ul>
   */
  VERSION("--version", null, null, true, 0),

  /**
   * <p>
   * Option for specifying the INI file to initialize the Senzing API's with.
   * The parameter to this option should be a file path to an INI file.
   * Alternatively, one can specify {@link #INIT_FILE} or {@link #INIT_JSON}.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--ini-file {file-path}</code></li>
   *   <li>Command Line: <code>-iniFile {file-path}</code></li>
   *   <li>Environment: <code>SENZING_ENGINE_CONFIGURATION_INI_FILE="{file-path}"</code></li>
   * </ul>
   */
  INI_FILE("--ini-file",
           "SENZING_ENGINE_CONFIGURATION_INI_FILE",
           null, true, 1),

  /**
   * <p>
   * Option for specifying the JSON init file to initialize the Senzing API's
   * with.  The parameter to this option should be a file path to a JSON init
   * file.  Alternatively, one can specify {@link #INI_FILE} or
   * {@link #INIT_JSON}.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--init-file {file-path}</code></li>
   *   <li>Command Line: <code>-initFile {file-path}</code></li>
   *   <li>Environment: <code>SENZING_ENGINE_CONFIGURATION_JSON_FILE="{file-path}"</code></li>
   * </ul>
   */
  INIT_FILE("--init-file",
            "SENZING_ENGINE_CONFIGURATION_JSON_FILE",
            null, true, 1),

  /**
   * <p>
   * Option for specifying the JSON text to initialize the Senzing API's
   * with.  The parameter to this option should be the actual JSON text with
   * which to initialize.  Alternatively, one can specify {@link #INI_FILE} or
   * {@link #INIT_FILE}.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--init-json {json-text}</code></li>
   *   <li>Command Line: <code>-initJson {json-text}</code></li>
   *   <li>Environment: <code>SENZING_ENGINE_CONFIGURATION_JSON="{json-text}"</code></li>
   * </ul>
   */
  INIT_JSON("--init-json",
            "SENZING_ENGINE_CONFIGURATION_JSON",
            null, true, 1),

  /**
   * <p>
   * Option for specifying the module name to initialize the Senzing API's
   * with.  The default value is {@link SzReplicatorConstants#DEFAULT_MODULE_NAME}.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--module-name {module-name}</code></li>
   *   <li>Command Line: <code>-moduleName {module-name}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_MODULE_NAME="{module-name}"</code></li>
   * </ul>
   */
  MODULE_NAME("--module-name",
              "SENZING_REPLICATOR_MODULE_NAME",
              null, 1, DEFAULT_MODULE_NAME),

  /**
   * <p>
   * This presence of this option causes the Senzing G2 API's to be initialized
   * in verbose mode, but its absence causes the Senzing API's to be initialized
   * in standard mode (the default).  This option is used with {@link #INI_FILE},
   * {@link #INIT_FILE}, or {@link #INIT_JSON} to control the Senzing API
   * initialization.  A single parameter may optionally be specified as
   * <code>true</code> or <code>false</code> with <code>false</code> simulating
   * the absence of the option.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--verbose [true|false]</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_VERBOSE="{true|false}"</code></li>
   * </ul>
   */
  VERBOSE("--verbose",
          "SENZING_REPLICATOR_VERBOSE",
          null, 0, "false"),

  /**
   * <p>
   * This option sets the number of threads available for executing Senzing API
   * functions (i.e.: the number of engine threads).  The single parameter to
   * this option should be a positive integer.  If not specified, then this
   * defaults to {@link SzReplicatorConstants#DEFAULT_CONCURRENCY}.  The
   * concurrency of each other component in the system is scaled from this
   * setting so more threads will exist, but only this specified number will
   * do with the native Senzing API.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--concurrency {thread-count}</code></li>
   *   <li>Command Line: <code>-concurrency {thread-count}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_CONCURRENCY="{thread-count}"</code></li>
   * </ul>
   */
  CONCURRENCY("--concurrency", ENV_PREFIX + "CONCURRENCY",
              null, 1, DEFAULT_CONCURRENCY_PARAM),

  /**
   * <p>
   * This option is used to specify the URL to an Amazon SQS queue to be used
   * for obtaining the info messages.  The single parameter to this option is
   * the URL.  If this option is specified then the info queue parameters for
   * RabbitMQ and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--sqs-info-url {url}</code></li>
   *   <li>Environment: <code>SENZING_SQS_INFO_URL="{url}"</code></li>
   * </ul>
   */
  SQS_INFO_URL("--sqs-info-url",
               "SENZING_SQS_INFO_QUEUE_URL", null, 1),

  /**
   * <p>
   * This option is used to specify the user name for connecting to RabbitMQ as
   * part of specifying a RabbitMQ info queue.  The single parameter to this
   * option is a user name.  If this option is specified then the other options
   * required for a RabbitMQ info queue are required and the info queue
   * parameters pertaining to SQS and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--rabbit-info-host {username}</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_INFO_USERNAME="{username}"</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_USERNAME="{username}" (fallback)</code></li>
   * </ul>
   */
  RABBIT_INFO_USER(
      "--rabbit-info-user",
      "SENZING_RABBITMQ_INFO_USERNAME",
      List.of("SENZING_RABBITMQ_USERNAME"), 1),

  /**
   * <p>
   * This option is used to specify the password for connecting to RabbitMQ as
   * part of specifying a RabbitMQ info queue.  The single parameter to this
   * option is a password.  If this option is specified then the other options
   * required for a RabbitMQ info queue are required and the info queue
   * parameters pertaining to SQS and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--rabbit-info-password {password}</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_INFO_PASSWORD="{password}"</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_PASSWORD="{password}" (fallback)</code></li>
   * </ul>
   */
  RABBIT_INFO_PASSWORD(
      "--rabbit-info-password",
      "SENZING_RABBITMQ_INFO_PASSWORD",
      List.of("SENZING_RABBITMQ_PASSWORD"), 1),

  /**
   * <p>
   * This option is used to specify the hostname for connecting to RabbitMQ as
   * part of specifying a RabbitMQ info queue.  The single parameter to this
   * option is a hostname or IP address.  If this option is specified then the
   * other options required for a RabbitMQ info queue are required and the
   * info queue parameters pertaining to SQS and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--rabbit-info-host {hostname}</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_INFO_HOST="{hostname}"</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_HOST="{hostname}" (fallback)</code></li>
   * </ul>
   */
  RABBIT_INFO_HOST(
      "--rabbit-info-host",
      "SENZING_RABBITMQ_INFO_HOST",
      List.of("SENZING_RABBITMQ_HOST"), 1),

  /**
   * <p>
   * This option is used to specify the port number for connecting to RabbitMQ
   * as part of specifying a RabbitMQ info queue.  The single parameter to this
   * option is a port number.  If this option is specified then the other
   * options required for a RabbitMQ info queue are required and the info queue
   * parameters pertaining to SQS and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--rabbit-info-port {port}</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_INFO_PORT="{port}"</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_PORT="{port}" (fallback)</code></li>
   * </ul>
   */
  RABBIT_INFO_PORT(
      "--rabbit-info-port",
      "SENZING_RABBITMQ_INFO_PORT",
      List.of("SENZING_RABBITMQ_PORT"), 1),

  /**
   * <p>
   * This option is used to specify the virtual host for connecting to RabbitMQ
   * as part of specifying a RabbitMQ info queue.  The single parameter to this
   * option is a virtual host name.  If this option is specified then the other
   * options required for a RabbitMQ info queue are required and the info queue
   * parameters pertaining to SQS and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--rabbit-info-virtual-host {virtual-host}</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_INFO_VIRTUAL_HOST="{virtual-host}"</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_VIRTUAL_HOST="{virtual-host}" (fallback)</code></li>
   * </ul>
   */
  RABBIT_INFO_VIRTUAL_HOST(
      "--rabbit-info-virtual-host",
      "SENZING_RABBITMQ_INFO_VIRTUAL_HOST",
      List.of("SENZING_RABBITMQ_VIRTUAL_HOST"), 1),

  /**
   * <p>
   * This option is used to specify the routing key for connecting to RabbitMQ
   * as part of specifying a RabbitMQ info queue.  The single parameter to this
   * option is a routing key.  If this option is specified then the other
   * options required for a RabbitMQ info queue are required and the info queue
   * parameters pertaining to SQS and Kafka are not allowed.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--rabbit-info-queue {queue-name}</code></li>
   *   <li>Environment: <code>SENZING_RABBITMQ_INFO_QUEUE="{queue-name}"</code></li>
   * </ul>
   */
  RABBIT_INFO_QUEUE(
      "--rabbit-info-queue",
      "SENZING_RABBITMQ_INFO_QUEUE",
      null, 1),

  /**
   * <p>
   * This option is used to specify the SQLite database file to connect to for
   * the data mart.  The single parameter to this option is the file path to
   * the SQLite database file to use.  If this option is specified the database
   * options for other database types cannot be specified.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--sqlite-database-file {file-path}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_SQLITE_DATABASE_FILE="{file-path}"</code></li>
   * </ul>
   */
  SQLITE_DATABASE_FILE(
      "--sqlite-database-file",
      "SENZING_REPLICATOR_SQLITE_DATABASE_FILE",
      null, 1),

  /**
   * <p>
   * This option is used to specify the PostgreSQL database server host to
   * connect to for the data mart.  The single parameter to this option is
   * server host name or IP address for the PostgreSQL database server.  If
   * this option is specified the database options for other database types
   * (e.g.: {@link #SQLITE_DATABASE_FILE}) cannot be specified and the other
   * PostgreSQL options (e.g.: {@link #POSTGRESQL_PORT}) are required.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--postgresql-host {host-name|ip-address}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_POSTGRESQL_HOST="{host-name|ip-address}"</code></li>
   * </ul>
   *
   * @see #POSTGRESQL_PORT
   * @see #POSTGRESQL_DATABASE
   * @see #POSTGRESQL_USER
   * @see #POSTGRESQL_PORT
   */
  POSTGRESQL_HOST(
      "--postgresql-host",
      "SENZING_REPLICATOR_POSTGRESQL_HOST",
      null, 1),

  /**
   * <p>
   * This option is used to specify the PostgreSQL database server port to
   * connect to for the data mart.  The single parameter to this option is the
   * port number for the PostgreSQL database server.  If this option is
   * specified the database options for other database types (e.g.: {@link
   * #SQLITE_DATABASE_FILE}) cannot be specified and the other PostgreSQL
   * options (e.g.: {@link #POSTGRESQL_PORT}) are required.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--postgresql-port {port-number}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_POSTGRESQL_PORT="{port-number}"</code></li>
   * </ul>
   *
   * @see #POSTGRESQL_HOST
   * @see #POSTGRESQL_DATABASE
   * @see #POSTGRESQL_USER
   * @see #POSTGRESQL_PORT
   */
  POSTGRESQL_PORT(
      "--postgresql-port",
      "SENZING_REPLICATOR_POSTGRESQL_PORT",
      null, 1),

  /**
   * <p>
   * This option is used to specify the PostgreSQL database name for the data
   * mart.  The single parameter to this option is the PostgreSQL database name
   * for the data mart.  If this option is specified the database options for
   * other database types (e.g.: {@link #SQLITE_DATABASE_FILE}) cannot be
   * specified and the other PostgreSQL options (e.g.: {@link
   * #POSTGRESQL_HOST}) are required.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--postgresql-database {database-name}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_POSTGRESQL_DATABASE="{database-name}"</code></li>
   * </ul>
   *
   * @see #POSTGRESQL_HOST
   * @see #POSTGRESQL_PORT
   * @see #POSTGRESQL_USER
   * @see #POSTGRESQL_PORT
   *
   */
  POSTGRESQL_DATABASE(
      "--postgresql-database",
      "SENZING_REPLICATOR_POSTGRESQL_DATABASE",
      null, 1),

  /**
   * <p>
   * This option is used to specify the PostgreSQL database user name to login
   * to the PostgreSQL database server for the data mart.  The single parameter
   * to this option is the user name for the PostgreSQL database server.  If
   * this option is specified the database options for other database types
   * (e.g.: {@link #SQLITE_DATABASE_FILE}) cannot be specified and the other
   * PostgreSQL options (e.g.: {@link #POSTGRESQL_HOST}) are required.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--postgresql-user {user-name}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_POSTGRESQL_USER="{user-name}"</code></li>
   * </ul>
   *
   * @see #POSTGRESQL_HOST
   * @see #POSTGRESQL_PORT
   * @see #POSTGRESQL_DATABASE
   * @see #POSTGRESQL_PORT
   */
  POSTGRESQL_USER(
      "--postgresql-user",
      "SENZING_REPLICATOR_POSTGRESQL_USER",
      null, 1),

  /**
   * <p>
   * This option is used to specify the PostgreSQL database user password to
   * login to the PostgreSQL database server for the data mart.  The single
   * parameter to this option is the password for the PostgreSQL database user.
   * If this option is specified the database options for other database types
   * (e.g.: {@link #SQLITE_DATABASE_FILE}) cannot be specified and the other
   * PostgreSQL options (e.g.: {@link #POSTGRESQL_HOST}) are required.
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <code>--postgresql-password {password}</code></li>
   *   <li>Environment: <code>SENZING_REPLICATOR_POSTGRESQL_PASSWORD="{password}"</code></li>
   * </ul>
   *
   * @see #POSTGRESQL_HOST
   * @see #POSTGRESQL_PORT
   * @see #POSTGRESQL_DATABASE
   * @see #POSTGRESQL_USER
   */
  POSTGRESQL_PASSWORD(
      "--postgresql-password",
      "SENZING_REPLICATOR_POSTGRESQL_PASSWORD",
      null, 1);

  /**
   * Constructs with the specified parameters.
   *
   * @param cmdLineFlag    The command-line flag.
   * @param envVariable    The primary environment variable.
   * @param envFallbacks   The {@link List} of fallback environment variables.
   * @param parameterCount The number of parameters for the option.
   */
  SzReplicatorOption(String       cmdLineFlag,
                     String       envVariable,
                     List<String> envFallbacks,
                     int          parameterCount)
  {
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
  SzReplicatorOption(String       cmdLineFlag,
                     String       envVariable,
                     List<String> envFallbacks,
                     int          parameterCount,
                     String...    defaultParameters)
  {
    this(cmdLineFlag,
         envVariable,
         envFallbacks,
         false,
         parameterCount,
         defaultParameters);
  }

  /**
   * Constructs with the specified parameters.
   *
   * @param cmdLineFlag       The command-line flag.
   * @param envVariable       The primary environment variable.
   * @param envFallbacks      The {@link List} of fallback environment variables.
   * @param parameterCount    The number of parameters for the option.
   * @param defaultParameters The default parameter value for the option if not
   *                          specified.
   */
  SzReplicatorOption(String       cmdLineFlag,
                     String       envVariable,
                     List<String> envFallbacks,
                     boolean      primary,
                     int          parameterCount,
                     String...    defaultParameters)
  {
    this.primary = primary;
    this.cmdLineFlag = cmdLineFlag;
    this.envVariable = envVariable;
    this.minParamCount = (parameterCount < 0) ? 0 : parameterCount;
    this.maxParamCount = parameterCount;
    this.envFallbacks = (envFallbacks == null)
        ? null : List.copyOf(envFallbacks);
    this.defaultParameters = (defaultParameters == null)
        ? Collections.emptyList() : List.of(defaultParameters);
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
   * The fallback environment variables for the option in descending
   * priority order.
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
      case RABBIT_INFO_PASSWORD:
      case POSTGRESQL_PASSWORD:
        return true;
      default:
        return false;
    }
  }

  static {
    Map<SzReplicatorOption, Set<Set<CommandLineOption>>> dependencyMap
        = new LinkedHashMap<>();
    Map<SzReplicatorOption, Set<CommandLineOption>> conflictMap
        = new LinkedHashMap<>();
    Map<String, SzReplicatorOption> lookupMap = new LinkedHashMap<>();

    try {
      // iterate over the options
      for (SzReplicatorOption option : SzReplicatorOption.values()) {
        conflictMap.put(option, new LinkedHashSet<>());
        dependencyMap.put(option, new LinkedHashSet<>());
        lookupMap.put(option.getCommandLineFlag().toLowerCase(), option);
      }

      SzReplicatorOption[] exclusiveOptions = {HELP, VERSION};
      for (SzReplicatorOption option : SzReplicatorOption.values()) {
        for (SzReplicatorOption exclOption : exclusiveOptions) {
          if (option == exclOption) continue;
          Set<CommandLineOption> set = conflictMap.get(exclOption);
          set.add(option);
          set = conflictMap.get(option);
          set.add(exclOption);
        }
      }

      SzReplicatorOption[] initOptions
          = {INI_FILE, INIT_FILE, INIT_JSON};

      // handle the messaging options
      Set<SzReplicatorOption> rabbitInfoOptions = Set.of(
          RABBIT_INFO_USER,
          RABBIT_INFO_PASSWORD,
          RABBIT_INFO_HOST,
          RABBIT_INFO_PORT,
          RABBIT_INFO_VIRTUAL_HOST,
          RABBIT_INFO_QUEUE);

      Set<CommandLineOption> requiredRabbit = Set.of(RABBIT_INFO_USER,
                                                     RABBIT_INFO_PASSWORD,
                                                     RABBIT_INFO_HOST,
                                                     RABBIT_INFO_QUEUE);

      Set<CommandLineOption> sqsInfoOptions = Set.of(SQS_INFO_URL);

      // enforce that we only have one info queue
      for (SzReplicatorOption option : rabbitInfoOptions) {
        Set<CommandLineOption> conflictSet = conflictMap.get(option);
        conflictSet.addAll(sqsInfoOptions);
      }
      for (CommandLineOption option : sqsInfoOptions) {
        Set<CommandLineOption> conflictSet = conflictMap.get(option);
        conflictSet.addAll(rabbitInfoOptions);
      }

      // make the optional rabbit options dependent on the required ones
      for (SzReplicatorOption option : rabbitInfoOptions) {
        if (requiredRabbit.contains(option)) continue;
        Set<Set<CommandLineOption>> dependencySets = dependencyMap.get(option);
        dependencySets.add(requiredRabbit);
      }

      // create a set of SQLite options
      Set<CommandLineOption> sqliteOptions = Set.of(SQLITE_DATABASE_FILE);

      // create the set of PostgreSQL options
      Set<CommandLineOption> postgreSqlOptions = Set.of(POSTGRESQL_HOST,
                                                        POSTGRESQL_PORT,
                                                        POSTGRESQL_DATABASE,
                                                        POSTGRESQL_USER,
                                                        POSTGRESQL_PASSWORD);

      Set<SzReplicatorOption> requiredPostgreSQL = Set.of(POSTGRESQL_HOST,
                                                          POSTGRESQL_DATABASE,
                                                          POSTGRESQL_USER,
                                                          POSTGRESQL_PASSWORD);

      // setup dependencies and conflicts for the PostgreSQL options
      for (CommandLineOption option: postgreSqlOptions) {
        // get the set of dependency sets
        Set<Set<CommandLineOption>> dependencySets = dependencyMap.get(option);

        // create a new set
        Set<CommandLineOption> dependSet = new LinkedHashSet<>();

        // add all required PostgreSQL options to the set
        dependSet.addAll(requiredPostgreSQL);

        // remove the current option
        dependSet.remove(option);

        // add the dependency set
        if (dependSet.size() > 0) {
          dependencySets.add(Collections.unmodifiableSet(dependSet));
        }

        // now set the conflicts
        conflictMap.put((SzReplicatorOption) option, sqliteOptions);
      }

      // setup conflicts for the database options
      conflictMap.put(SQLITE_DATABASE_FILE, postgreSqlOptions);

      List<Set<CommandLineOption>> baseDependSets = new LinkedList<>();
      Set<CommandLineOption> dependSet = new LinkedHashSet<>();
      dependSet.add(SQS_INFO_URL);
      dependSet.add(SQLITE_DATABASE_FILE);
      baseDependSets.add(Collections.unmodifiableSet(dependSet));

      dependSet = new LinkedHashSet<>();
      dependSet.add(SQS_INFO_URL);
      dependSet.addAll(requiredPostgreSQL);
      baseDependSets.add(Collections.unmodifiableSet(dependSet));

      dependSet = new LinkedHashSet<>();
      dependSet.add(SQLITE_DATABASE_FILE);
      dependSet.addAll(requiredRabbit);
      baseDependSets.add(Collections.unmodifiableSet(dependSet));

      dependSet = new LinkedHashSet<>();
      dependSet.addAll(requiredPostgreSQL);
      dependSet.addAll(requiredRabbit);
      baseDependSets.add(Collections.unmodifiableSet(dependSet));

      // make the primary options dependent on one set of info queue options
      for (SzReplicatorOption option : initOptions) {
        Set<Set<CommandLineOption>> dependencySets = dependencyMap.get(option);

        dependencySets.addAll(baseDependSets);
      }

    } catch (Exception e) {
      e.printStackTrace();
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
     * @throws IllegalArgumentException If the specified {@link
     *                                  CommandLineOption} is not an instance of
     *                                  {@link SzReplicatorOption} or is otherwise
     *                                  unrecognized.
     */
    public Object process(CommandLineOption option, List<String> params)
    {
      if (!(option instanceof SzReplicatorOption)) {
        throw new IllegalArgumentException(
            "Unhandled command line option: " + option.getCommandLineFlag()
                + " / " + option);
      }

      // down-cast
      SzReplicatorOption replicatorOption = (SzReplicatorOption) option;
      switch (replicatorOption) {
        case HELP:
        case VERSION:
          return Boolean.TRUE;

        case VERBOSE:
          if (params.size() == 0) return Boolean.TRUE;
          String boolText = params.get(0);
          if ("false".equalsIgnoreCase(boolText)) {
            return Boolean.FALSE;
          }
          if ("true".equalsIgnoreCase(boolText)) {
            return Boolean.TRUE;
          }
          throw new IllegalArgumentException(
              "The specified parameter for "
                  + option.getCommandLineFlag()
                  + " must be true or false: " + params.get(0));

        case INI_FILE:
          File iniFile = new File(params.get(0));
          if (!iniFile.exists()) {
            throw new IllegalArgumentException(
                "Specified INI file does not exist: " + iniFile);
          }
          return iniFile;

        case INIT_FILE:
          File initFile = new File(params.get(0));
          if (!initFile.exists()) {
            throw new IllegalArgumentException(
                "Specified JSON init file does not exist: " + initFile);
          }
          String jsonText;
          try {
            jsonText = readTextFileAsString(initFile, "UTF-8");

          } catch (IOException e) {
            throw new RuntimeException(
                multilineFormat(
                    "Failed to read JSON initialization file: "
                        + initFile,
                    "",
                    "Cause: " + e.getMessage()));
          }
          try {
            return JsonUtilities.parseJsonObject(jsonText);

          } catch (Exception e) {
            throw new IllegalArgumentException(
                "The initialization file does not contain valid JSON: "
                    + initFile);
          }

        case INIT_JSON:
          String initJson = params.get(0);
          if (initJson.trim().length() == 0) {
            throw new IllegalArgumentException(
                "Initialization JSON is missing or empty.");
          }
          try {
            return JsonUtilities.parseJsonObject(initJson);

          } catch (Exception e) {
            throw new IllegalArgumentException(
                multilineFormat(
                    "Initialization JSON is not valid JSON: ",
                    initJson));
          }

        case CONCURRENCY: {
          int threadCount;
          try {
            threadCount = Integer.parseInt(params.get(0));
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Thread count must be an integer: " + params.get(0));
          }
          if (threadCount <= 0) {
            throw new IllegalArgumentException(
                "Negative thread counts are not allowed: " + threadCount);
          }
          return threadCount;
        }

        case MODULE_NAME:
        case SQS_INFO_URL:
        case RABBIT_INFO_HOST:
        case RABBIT_INFO_USER:
        case RABBIT_INFO_PASSWORD:
        case RABBIT_INFO_VIRTUAL_HOST:
        case RABBIT_INFO_QUEUE:
        case POSTGRESQL_HOST:
        case POSTGRESQL_DATABASE:
        case POSTGRESQL_USER:
        case POSTGRESQL_PASSWORD:
          return params.get(0);

        case RABBIT_INFO_PORT:
        case POSTGRESQL_PORT: {
          int port = Integer.parseInt(params.get(0));
          if (port < 0) {
            throw new IllegalArgumentException(
                "Negative port numbers are not allowed: " + port);
          }
          return port;
        }

        default:
          throw new IllegalArgumentException(
              "Unhandled command line option: "
                  + option.getCommandLineFlag()
                  + " / " + option);

      }
    }
  }

  /**
   * The {@link ParameterProcessor} for {@link SzReplicatorOption}.
   * This instance will only handle instances of {@link CommandLineOption}
   * instances of type {@link SzReplicatorOption}.
   */
  public static final ParameterProcessor PARAMETER_PROCESSOR
      = new ParamProcessor();

}
