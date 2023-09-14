package com.senzing.datamart;

import com.senzing.cmdline.*;
import com.senzing.g2.engine.G2Product;
import com.senzing.g2.engine.G2ProductJNI;
import com.senzing.listener.communication.MessageConsumer;
import com.senzing.listener.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.communication.sqs.SQSConsumer;
import com.senzing.listener.service.ListenerService;
import com.senzing.listener.service.g2.G2Service;
import com.senzing.listener.service.scheduling.AbstractSQLSchedulingService;
import com.senzing.listener.service.scheduling.AbstractSchedulingService;
import com.senzing.listener.service.scheduling.PostgreSQLSchedulingService;
import com.senzing.listener.service.scheduling.SQLiteSchedulingService;
import com.senzing.text.TextUtilities;
import com.senzing.util.AccessToken;
import com.senzing.util.JsonUtilities;
import com.senzing.sql.*;
import com.senzing.util.LoggingUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Supplier;

import static com.senzing.cmdline.CommandLineUtilities.getJarName;
import static com.senzing.datamart.SzReplicatorOption.*;
import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.util.Quantified.*;
import static com.senzing.listener.service.scheduling.AbstractSchedulingService.Stat.*;
import static com.senzing.listener.communication.AbstractMessageConsumer.Stat.*;
import static com.senzing.sql.ConnectionPool.Stat.*;

/**
 * The data mart replicator command-line class.
 */
public class SzReplicator extends Thread {
  /**
   * The name of the JAR file for command-line excecution.
   */
  private static final String JAR_FILE_NAME = getJarName(SzReplicator.class);

  /**
   * The date-time pattern for the build number.
   */
  private static final String BUILD_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss z";

  /**
   * The time zone used for the time component of the build number.
   */
  private static final ZoneId BUILD_ZONE = ZoneId.of("America/Los_Angeles");

  /**
   * The {@link DateTimeFormatter} for interpretting the build number as a
   * LocalDateTime instance.
   */
  private static final DateTimeFormatter BUILD_DATE_FORMATTER
      = DateTimeFormatter.ofPattern(BUILD_DATE_PATTERN).withZone(BUILD_ZONE);

  /**
   * The date-time pattern for the build number.
   */
  private static final String BUILD_NUMBER_PATTERN = "yyyy_MM_dd__HH_mm";

  /**
   * The {@link DateTimeFormatter} for interpreting the build number as a
   * LocalDateTime instance.
   */
  private static final DateTimeFormatter BUILD_NUMBER_FORMATTER
      = DateTimeFormatter.ofPattern(BUILD_NUMBER_PATTERN);

  /**
   * The {@link String} token used to identify development builds when parsing
   * the version info.
   */
  private static final String DEVELOPMENT_VERSION_TOKEN = "DEVELOPMENT_VERSION";

  /**
   * The main function for command-line starting.
   *
   * @param args The command-line arguments.
   *
   * @throws Exception If a failure occurs.
   */
  public static void main(String[] args) throws Exception {
    commandLineStart(args,
                     SzReplicator::parseCommandLine,
                     SzReplicator::getUsageString,
                     SzReplicator::getVersionString,
                     SzReplicator::build);
  }

  /**
   * Parses the {@link SzReplicator} command line arguments and produces a
   * {@link Map} of {@link CommandLineOption} keys to {@link Object} command
   * line values.
   *
   * @param args The arguments to parse.
   * @param deprecationWarnings The {@link List} to populate with deprecation
   *                            warnings if any are found in the command line.
   * @return The {@link Map} describing the command-line arguments.
   * @throws CommandLineException If a command-line parsing failure occurs.
   */
  protected static Map<CommandLineOption, Object> parseCommandLine(
      String[] args, List<DeprecatedOptionWarning> deprecationWarnings)
      throws CommandLineException
  {
    Map<CommandLineOption, CommandLineValue> optionValues
        = CommandLineUtilities.parseCommandLine(
        SzReplicatorOption.class,
        args,
        SzReplicatorOption.PARAMETER_PROCESSOR,
        deprecationWarnings);

    // iterate over the option values and handle them
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("message", "Startup Options");

    StringBuilder sb = new StringBuilder();

    Map<CommandLineOption, Object> result = new LinkedHashMap<>();

    CommandLineUtilities.processCommandLine(optionValues, result, job, sb);

    // log the options
    if (!optionValues.containsKey(HELP) && !optionValues.containsKey(VERSION)) {
      System.out.println(
          "[" + (new Date()) + "] Senzing Data Mart Replicator: "
              + sb.toString());
    }

    // return the result
    return result;
  }

  /**
   * Creates a new instance of {@link SzReplicator} from the specified options.
   *
   * @param options The {@link Map} of {@link CommandLineOption} keys to
   *                {@link Object} values.
   * @return The created instance of {@link SzReplicator}.
   * @throws Exception If a failure occurs.
   */
  private static SzReplicator build(Map<CommandLineOption, Object> options)
      throws Exception
  {
    return new SzReplicator(options);
  }

  /**
   * Starts an instance of the server from the command line using the specified
   * arguments and the specified function helpers.
   *
   * @param args           The command-line arguments.
   * @param cmdLineParser  The {@link CommandLineParser} to parse the command
   *                       line arguments.
   * @param usageMessage   The {@link Supplier} for the usage message.
   * @param versionMessage The {@link Supplier} for the version message.
   * @param appBuilder     The {@link CommandLineBuilder} to create the server
   *                       instance from the command line options.
   * @throws Exception If a failure occurs.
   */
  protected static void commandLineStart(
      String[]                        args,
      CommandLineParser               cmdLineParser,
      Supplier<String>                usageMessage,
      Supplier<String>                versionMessage,
      CommandLineBuilder<SzReplicator>  appBuilder)
      throws Exception
  {
    Map<CommandLineOption, Object> options   = null;
    List<DeprecatedOptionWarning> warnings  = new LinkedList<>();
    try {
      options = cmdLineParser.parseCommandLine(args, warnings);

      for (DeprecatedOptionWarning warning: warnings) {
        System.out.println(warning);
        System.out.println();
      }

    } catch (CommandLineException e) {
      System.out.println();
      System.out.println(e.getMessage());

      System.err.println();
      System.err.println(
          "Try the " + HELP.getCommandLineFlag() + " option for help.");
      System.err.println();
      System.exit(1);

    } catch (Exception e) {
      if (!isLastLoggedException(e)) {
        System.err.println();
        System.err.println(e.getMessage());
        System.err.println();
        e.printStackTrace();
      }
      System.exit(1);
    }

    if (options.containsKey(HELP)) {
      System.out.println(usageMessage.get());
      System.exit(0);
    }
    if (options.containsKey(VERSION)) {
      System.out.println();
      System.out.println(versionMessage.get());
      System.exit(0);
    }

    System.out.println("os.arch        = " + System.getProperty("os.arch"));
    System.out.println("os.name        = " + System.getProperty("os.name"));
    System.out.println("user.dir       = " + System.getProperty("user.dir"));
    System.out.println("user.home      = " + System.getProperty("user.home"));
    System.out.println("java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));

    SzReplicator replicator = null;
    try {
      System.err.println();
      System.err.println(options);
      System.err.println();
      replicator = appBuilder.build(options);

      if (replicator == null) {
        System.err.println("FAILED TO INITIALIZE");
        System.exit(0);
      }

    } catch (Exception e) {
      e.printStackTrace();
      exitOnError(e);
    }

    logInfo("STARTING REPLICATOR...");
    replicator.start();
    logInfo("STARTED REPLICATOR.");
    logInfo("JOINING REPLICATOR...");
    replicator.join();
    logInfo("JOINED REPLICATOR.");
  }

  /**
   *
   */
  public void run() {
    try {
      this.startTimeNanos = System.nanoTime();
      logInfo("STARTING MESSAGE CONSUMPTION...");
      this.messageConsumer.consume(this.replicatorService);
      logInfo("MESSAGE CONSUMPTION STARTED.");

      final Object monitor = new Object();
      synchronized (monitor) {
        while (true) {
          monitor.wait(60000L);
          ListenerService.State listenerState
              = this.replicatorService.getState();

          MessageConsumer.State consumerState
              = this.messageConsumer.getState();

          // check if something has interrupted processing
          if (listenerState != ListenerService.State.AVAILABLE
              || consumerState != MessageConsumer.State.CONSUMING) {
            break;
          }

          this.printStatistics();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      this.messageConsumer.destroy();
      this.replicatorService.destroy();
    }
  }

  /**
   *
   */
  private void printStatistics() {
    Map<Statistic, Number> stats = new LinkedHashMap<>();
    stats.putAll(this.messageConsumer.getStatistics());
    stats.putAll(this.replicatorService.getStatistics());
    stats.putAll(this.connPool.getStatistics());

    long    now     = System.nanoTime();
    double  elapsed = ((double) (now - this.startTimeNanos)) / 1000000000.0;

    Number completeCount = stats.get(taskGroupCompleteCount);
    long   completed = completeCount.longValue();

    MessageConsumer     consumer  = this.messageConsumer;
    SzReplicatorService service   = this.replicatorService;

    System.out.println();
    System.out.println("=====================================================");
    System.out.println("STATISTICS:");
    printStatisticsMap(stats);
  }

  /**
   *
   */
  private static void printStatisticsMap(Map<Statistic,Number> stats) {
    stats.forEach((key, value) -> {
      String units = key.getUnits();
      System.out.println("  " + key.getName() + ": " + value
                             + ((units != null) ? " " + units : ""));
    });
  }

  /**
   * Returns a formatted string describing the version details of the
   * API Server.
   *
   * @return A formatted string describing the version details.
   */
  protected static String getVersionString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    printJarVersion(pw);
    printSenzingVersions(pw);
    return sw.toString();
  }

  /**
   * Prints the JAR version header for the version string to the specified
   * {@link PrintWriter}.
   *
   * @param pw The {@link PrintWriter} to print the version information to.
   */
  protected static void printJarVersion(PrintWriter pw) {
    pw.println("[ " + JAR_FILE_NAME + " version "
                   + BuildInfo.MAVEN_VERSION + " ]");
  }

  /**
   * Prints the Senzing version information to the specified {@link
   * PrintWriter}.
   *
   * @param pw The {@link PrintWriter} to print the version information to.
   */
  protected static void printSenzingVersions(PrintWriter pw) {
    // use G2Product API without "init()" for now
    G2Product productApi = new G2ProductJNI();
    String jsonText = productApi.version();
    JsonObject jsonObj = JsonUtilities.parseJsonObject(jsonText);


    String nativeApiVersion = JsonUtilities.getString(jsonObj, "VERSION");
    String buildVersion     = JsonUtilities.getString(jsonObj, "BUILD_VERSION");
    String buildNumber      = JsonUtilities.getString(jsonObj, "BUILD_NUMBER");

    Date buildDate = null;
    if (buildNumber != null && buildNumber.length() > 0
        && buildNumber.indexOf(DEVELOPMENT_VERSION_TOKEN) < 0)
    {
      LocalDateTime localDateTime = LocalDateTime.parse(buildNumber,
                                                        BUILD_NUMBER_FORMATTER);
      ZonedDateTime zonedDateTime = localDateTime.atZone(BUILD_ZONE);
      buildDate = Date.from(zonedDateTime.toInstant());

    } else {
      buildDate = new Date();
    }

    String formattedBuildDate = BUILD_DATE_FORMATTER.format(
        Instant.ofEpochMilli(buildDate.getTime()));

    JsonObject compatVersion
        = JsonUtilities.getJsonObject(jsonObj, "COMPATIBILITY_VERSION");

    String configCompatVersion = JsonUtilities.getString(compatVersion,
                                                     "CONFIG_VERSION");

    pw.println(" - Senzing Replicator Version   : " + BuildInfo.MAVEN_VERSION);
    pw.println(" - Senzing Native API Version   : " + nativeApiVersion);
    pw.println(" - Senzing Native Build Version : " + buildVersion);
    pw.println(" - Senzing Native Build Number  : " + buildNumber);
    pw.println(" - Senzing Native Build Date    : " + formattedBuildDate);
    pw.println(" - Config Compatibility Version : " + configCompatVersion);
    pw.flush();
  }

  /**
   * Prints the introduction to the usage message to the specified {@link
   * PrintWriter}.
   *
   * @param pw The {@link PrintWriter} to write the usage introduction to.
   */
  protected static void printUsageIntro(PrintWriter pw) {
    pw.println(multilineFormat(
        "java -jar " + JAR_FILE_NAME + " <options>",
        "",
        "<options> includes: "));
  }

  /**
   * Prints the usage for the standard options to the specified {@link
   * PrintWriter}.
   *
   * @param pw The {@link PrintWriter} to write the standard options usage.
   */
  protected static void printStandardOptionsUsage(PrintWriter pw) {
    pw.println(multilineFormat(
        "[ Standard Options ]",
        "   --help",
        "        Should be the first and only option if provided.  Causes this help",
        "        message to be displayed.",
        "        NOTE: If this option is provided, the replicator will not start.",
        "",
        "   --version",
        "        Should be the first and only option if provided.  Causes the version",
        "        of the G2 REST API Server to be displayed.",
        "        NOTE: If this option is provided, the replicator will not start.",
        "",
        "   --concurrency <thread-count>",
        "        Sets the number of threads available for performing Senzing API",
        "        operations (i.e.: the number of engine threads).  The number of",
        "        threads for consuming messages and handling tasks is scaled based",
        "        on the engine concurrency.  If not specified, then this defaults to "
            + DEFAULT_CONCURRENCY + ".",
        "        --> VIA ENVIRONMENT: " + CONCURRENCY.getEnvironmentVariable(),
        "",
        "   --module-name <module-name>",
        "        The module name to initialize with.  If not specified, then the module",
        "        name defaults to \"" + DEFAULT_MODULE_NAME + "\".",
        "        --> VIA ENVIRONMENT: " + MODULE_NAME.getEnvironmentVariable(),
        "",
        "   --verbose [true|false]",
        "        Also -verbose.  If specified then initialize in verbose mode.  The",
        "        true/false parameter is optional, if not specified then true is assumed.",
        "        If specified as false then it is the same as omitting the option with",
        "        the exception that omission falls back to the environment variable",
        "        setting whereas an explicit false overrides any environment variable.",
        "        --> VIA ENVIRONMENT: " + VERBOSE.getEnvironmentVariable(),
        "",
        "   --ini-file <ini-file-path>",
        "        The path to the Senzing INI file to with which to initialize.",
        "        EXAMPLE: -iniFile /etc/opt/senzing/G2Module.ini",
        "        --> VIA ENVIRONMENT: " + INI_FILE.getEnvironmentVariable(),
        "",
        "   --init-file <json-init-file>",
        "        The path to the file containing the JSON text to use for Senzing",
        "        initialization.  EXAMPLE: -initFile ~/senzing/g2-init.json",
        "        --> VIA ENVIRONMENT: " + INIT_FILE.getEnvironmentVariable(),
        "",
        "   --init-json <json-init-text>",
        "        The JSON text to use for Senzing initialization.",
        "        *** SECURITY WARNING: If the JSON text contains a password",
        "        then it may be visible to other users via process monitoring.",
        "        EXAMPLE: -initJson \"{\"PIPELINE\":{ ... }}\"",
        "        --> VIA ENVIRONMENT: " + INIT_JSON.getEnvironmentVariable()));
  }

  /**
   * Prints the info-queue options usage to the specified {@link PrintWriter}.
   *
   * @param pw The {@link PrintWriter} to write the info-queue options usage.
   */
  protected static void printInfoQueueOptionsUsage(PrintWriter pw) {
    pw.println(multilineFormat(
        "[ Asynchronous Info Queue Options ]",
        "   The following options pertain to configuring the message queue from which to",
        "   receive the \"info\" messages.  Exactly one such queue must be configured.",
        "",
        "   --sqs-info-url <url>",
        "        Specifies an Amazon SQS queue URL as the info queue.",
        "        --> VIA ENVIRONMENT: " + SQS_INFO_URL.getEnvironmentVariable(),
        "",
        "   --rabbit-info-host <hostname>",
        "        Used to specify the hostname for connecting to RabbitMQ as part of",
        "        specifying a RabbitMQ info queue.",
        "        --> VIA ENVIRONMENT: " + RABBIT_INFO_HOST.getEnvironmentVariable(),
        "                             "
            + RABBIT_INFO_HOST.getEnvironmentFallbacks().iterator().next()
            + " (fallback)",
        "",
        "   --rabbit-info-port <port>",
        "        Used to specify the port number for connecting to RabbitMQ as part of",
        "        specifying a RabbitMQ info queue.",
        "        --> VIA ENVIRONMENT: " + RABBIT_INFO_PORT.getEnvironmentVariable(),
        "                             "
            + RABBIT_INFO_PORT.getEnvironmentFallbacks().iterator().next()
            + " (fallback)",
        "",
        "   --rabbit-info-user <user name>",
        "        Used to specify the user name for connecting to RabbitMQ as part of",
        "        specifying a RabbitMQ info queue.",
        "        --> VIA ENVIRONMENT: " + RABBIT_INFO_USER.getEnvironmentVariable(),
        "                             "
            + RABBIT_INFO_USER.getEnvironmentFallbacks().iterator().next()
            + " (fallback)",
        "",
        "   --rabbit-info-password <password>",
        "        Used to specify the password for connecting to RabbitMQ as part of",
        "        specifying a RabbitMQ info queue.",
        "        --> VIA ENVIRONMENT: " + RABBIT_INFO_PASSWORD.getEnvironmentVariable(),
        "                             "
            + RABBIT_INFO_PASSWORD.getEnvironmentFallbacks().iterator().next()
            + " (fallback)",
        "",
        "   --rabbit-info-virtual-host <virtual host>",
        "        Used to specify the virtual host for connecting to RabbitMQ as part of",
        "        specifying a RabbitMQ info queue.",
        "        --> VIA ENVIRONMENT: " + RABBIT_INFO_VIRTUAL_HOST.getEnvironmentVariable(),
        "                             "
            + RABBIT_INFO_VIRTUAL_HOST.getEnvironmentFallbacks().iterator().next()
            + " (fallback)",
        "",
        "   --rabbit-info-queue <queue name>",
        "        Used to specify the name of the RabbitMQ queue from which to pull the",
        "        info messages.",
        "        --> VIA ENVIRONMENT: " + RABBIT_INFO_QUEUE.getEnvironmentVariable()));
  }

  /**
   * Prints the data-mart database connectivity options usage to the specified
   * {@link PrintWriter}.
   *
   * @param pw The {@link PrintWriter} to write the data-mart database
   *           connectivity options usage.
   */
  protected static void printDatabaseOptionsUsage(PrintWriter pw) {
    pw.println(multilineFormat(
        "[ Data Mart Database Connectivity Options ]",
        "   The following options pertain to configuring the connection to the data-mart",
        "   database.  Exactly one such database must be configured.",
        "",
        "   --sqlite-database-file <url>",
        "        Specifies an SQLite database file to open (or create) to use as the",
        "        data-mart database.  NOTE: SQLite may be used for testing, but because",
        "        only one connection may be made, it will not scale for production use.",
        "        --> VIA ENVIRONMENT: " + SQLITE_DATABASE_FILE.getEnvironmentVariable(),
        "",
        "   --postgresql-host <hostname>",
        "        Used to specify the hostname for connecting to PostgreSQL as the ",
        "        data-mart database.",
        "        --> VIA ENVIRONMENT: " + POSTGRESQL_HOST.getEnvironmentVariable(),
        "",
        "   --postgresql-port <port>",
        "        Used to specify the port number for connecting to PostgreSQL as the ",
        "        data-mart database.",
        "        --> VIA ENVIRONMENT: " + POSTGRESQL_PORT.getEnvironmentVariable(),
        "",
        "   --postgresql-database <database>",
        "        Used to specify the database name for connecting to PostgreSQL as the ",
        "        data-mart database.",
        "        --> VIA ENVIRONMENT: " + POSTGRESQL_DATABASE.getEnvironmentVariable(),
        "",
        "   --postgresql-user <user name>",
        "        Used to specify the user name for connecting to PostgreSQL as the ",
        "        data-mart database.",
        "        --> VIA ENVIRONMENT: " + POSTGRESQL_USER.getEnvironmentVariable(),
        "",
        "   --postgresql-password <password>",
        "        Used to specify the password for connecting to PostgreSQL as the ",
        "        data-mart database.",
        "        --> VIA ENVIRONMENT: " + POSTGRESQL_PASSWORD.getEnvironmentVariable()));
  }

  /**
   * Generates the usage string for the data-mart replicator.
   *
   * @return The usage string for the data-mart replicator.
   */
  public static String getUsageString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    printUsageIntro(pw);
    printStandardOptionsUsage(pw);
    printInfoQueueOptionsUsage(pw);
    printDatabaseOptionsUsage(pw);

    pw.flush();
    sw.flush();

    return sw.toString();
  }

  /**
   * Exits when the specified failure occurs.
   *
   * @param t The {@link Throwable} representing the failure.
   */
  private static void exitOnError(Throwable t) {
    logError(t, "EXITING ON ERROR:");
    System.exit(1);
  }

  /**
   * The G2 service use for interacting with the entity repository.
   */
  private G2Service g2Service;

  /**
   * The configured concurrency.
   */
  private int concurrency;

  /**
   * The start time when the {@link #run()} method is called.
   */
  private long startTimeNanos = 0L;

  /**
   * The {@link Connector} for connecting to the data-mart database.
   */
  private Connector connector = null;

  /**
   * The {@link ConnectionPool} to use for connecting.
   */
  private ConnectionPool connPool = null;

  /**
   * The {@link ConnectionProvider} that is backed by the
   * {@link ConnectionPool}.
   */
  private ConnectionProvider connProvider;

  /**
   * The unique name used to bind the {@link ConnectionProvider} in the
   * {@link ConnectionProvider#REGISTRY}.
   */
  private String connProviderName;

  /**
   * The {@link AccessToken} that was obtained when binding the
   * {@link ConnectionProvider} in the {@link ConnectionProvider#REGISTRY}.
   */
  private AccessToken connProviderToken;

  /**
   * The {@link MessageConsumer} to use.
   */
  private MessageConsumer messageConsumer = null;

  /**
   * The {@link SzReplicatorService} to use for
   */
  private SzReplicatorService replicatorService;

  /**
   * Constructs an instance of {@link SzReplicator} with the specified {@link
   * SzReplicatorOptions} instance.
   *
   * @param options The {@link SzReplicatorOptions} instance with which to
   *                construct the API server instance.
   * @throws Exception If a failure occurs.
   */
  public SzReplicator(SzReplicatorOptions options)
    throws Exception
  {
    this(options.buildOptionsMap());
  }

  /**
   * Constructs with the specified parameters.
   *
   * @param options The options with which to initialize.
   * @throws Exception If a failure occurs.
   */
  protected SzReplicator(Map<CommandLineOption, Object> options)
    throws Exception
  {
    this(null, options);
  }

  /**
   * Constructs an instance of {@link SzReplicator} with the specified {@link
   * SzReplicatorOptions} instance.
   *
   * @param accessToken The {@link AccessToken} for later accessing privileged
   *                    functions.
   * @param options     The {@link SzReplicatorOptions} instance with which to
   *                    construct the API server instance.
   * @throws Exception If a failure occurs.
   */
  protected SzReplicator(AccessToken                    accessToken,
                         Map<CommandLineOption, Object> options)
      throws Exception
  {
    // get the module name
    String moduleName = DEFAULT_MODULE_NAME;
    if (options.containsKey(MODULE_NAME)) {
      moduleName = (String) options.get(MODULE_NAME);
    }

    // get the concurrency
    this.concurrency = DEFAULT_CONCURRENCY;
    if (options.containsKey(CONCURRENCY)) {
      this.concurrency = (Integer) options.get(CONCURRENCY);
    }

    final int consumerConcurrency = this.concurrency * 2;
    final int scheduleConcurrency = this.concurrency * 2;
    final int poolSize            = this.concurrency;
    final int maxPoolSize         = poolSize * 3;
    final int g2Concurrency       = this.concurrency;

    // create the configuration for the G2Service
    JsonObjectBuilder g2ConfigBuilder = Json.createObjectBuilder();
    // determine the init JSON
    JsonObject initJson = (JsonObject) options.get(INIT_JSON);
    if (initJson == null) {
      initJson = (JsonObject) options.get(INIT_FILE);
    }
    if (initJson == null) {
      initJson = (JsonObject) options.get(INI_FILE);
    }
    g2ConfigBuilder.add(G2Service.G2_INIT_CONFIG_KEY, initJson);
    g2ConfigBuilder.add(G2Service.G2_MODULE_NAME_KEY, moduleName);
    boolean verbose = false;
    if (options.containsKey(VERBOSE)) {
      verbose = (Boolean) options.get(VERBOSE);
    }
    g2ConfigBuilder.add(G2Service.G2_VERBOSE_KEY, verbose);
    g2ConfigBuilder.add(G2Service.CONCURRENCY_KEY, g2Concurrency);

    // setup the connection pool / provider
    String schedulingServiceClassName = null;

    if (options.containsKey(SQLITE_DATABASE_FILE)) {
      File dbFile = (File) options.get(SQLITE_DATABASE_FILE);
      this.connector = new SQLiteConnector(dbFile);

      this.connPool = new ConnectionPool(this.connector, 1);

      schedulingServiceClassName = SQLiteSchedulingService.class.getName();

    } else {
      String  host      = (String) options.get(POSTGRESQL_HOST);
      Integer port      = (Integer) options.get(POSTGRESQL_PORT);
      String  database  = (String) options.get(POSTGRESQL_DATABASE);
      String  user      = (String) options.get(POSTGRESQL_USER);
      String  password  = (String) options.get(POSTGRESQL_PASSWORD);
      this.connector = new PostgreSqlConnector(host,
                                               port,
                                               database,
                                               user,
                                               password);

      this.connPool = new ConnectionPool(this.connector,
                                         TransactionIsolation.READ_COMMITTED,
                                         poolSize,
                                         maxPoolSize);

      schedulingServiceClassName = PostgreSQLSchedulingService.class.getName();
    }

    this.connProvider = new PoolConnectionProvider(this.connPool,
                                                   10000L);

    this.connProviderName = TextUtilities.randomAlphanumericText(30);

    this.connProviderToken = ConnectionProvider.REGISTRY.bind(
        this.connProviderName, this.connProvider);

    // handle the scheduling service config
    JsonObjectBuilder schedulingJOB = Json.createObjectBuilder();
    schedulingJOB.add(AbstractSchedulingService.CONCURRENCY_KEY,
                      scheduleConcurrency);
    schedulingJOB.add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY,
                      this.connProviderName);

    // handle the replicator config
    JsonObjectBuilder replicatorJOB = Json.createObjectBuilder();
    replicatorJOB.add(SzReplicatorService.SCHEDULING_SERVICE_CLASS_KEY,
                      schedulingServiceClassName);

    replicatorJOB.add(SzReplicatorService.SCHEDULING_SERVICE_CONFIG_KEY,
                      schedulingJOB);

    replicatorJOB.add(SzReplicatorService.G2_SERVICE_CONFIG_KEY,
                      g2ConfigBuilder);

    replicatorJOB.add(SzReplicatorService.CONNECTION_PROVIDER_KEY,
                      this.connProviderName);

    this.replicatorService = new SzReplicatorService();
    this.replicatorService.init(replicatorJOB.build());

    // build the message consumer
    JsonObjectBuilder consumerJOB = Json.createObjectBuilder();
    if (options.containsKey(RABBIT_INFO_HOST)) {
      consumerJOB.add(RabbitMQConsumer.CONCURRENCY_KEY, consumerConcurrency);
      consumerJOB.add(RabbitMQConsumer.MQ_HOST_KEY,
                      ((String) options.get(RABBIT_INFO_HOST)));
      consumerJOB.add(RabbitMQConsumer.MQ_USER_KEY,
                      ((String) options.get(RABBIT_INFO_USER)));
      consumerJOB.add(RabbitMQConsumer.MQ_PASSWORD_KEY,
                      ((String) options.get(RABBIT_INFO_PASSWORD)));
      consumerJOB.add(RabbitMQConsumer.MQ_QUEUE_KEY,
                      ((String) options.get(RABBIT_INFO_QUEUE)));

      // check if we have the port parameter
      if (options.containsKey(RABBIT_INFO_PORT)) {
        consumerJOB.add(RabbitMQConsumer.MQ_PORT_KEY,
                        ((Integer) options.get(RABBIT_INFO_PORT)));
      }

      // check if we have the virtual host parameter
      if (options.containsKey(RABBIT_INFO_VIRTUAL_HOST)) {
        consumerJOB.add(RabbitMQConsumer.MQ_VIRTUAL_HOST_KEY,
                        ((String) options.get(RABBIT_INFO_VIRTUAL_HOST)));
      }

      this.messageConsumer = new RabbitMQConsumer();

    } else {
      consumerJOB.add(SQSConsumer.CONCURRENCY_KEY, consumerConcurrency);

      // build an SQS message consumer
      consumerJOB.add(SQSConsumer.SQS_URL_KEY,
                      ((String) options.get(SQS_INFO_URL)));
      this.messageConsumer = new SQSConsumer();
    }
    this.messageConsumer.init(consumerJOB.build());
  }
}
