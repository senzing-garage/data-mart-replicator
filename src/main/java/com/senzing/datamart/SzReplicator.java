package com.senzing.datamart;

import com.senzing.cmdline.*;
import com.senzing.listener.communication.MessageConsumer;
import com.senzing.listener.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.communication.sqs.SQSConsumer;
import com.senzing.listener.communication.sql.SQLConsumer;
import com.senzing.listener.service.ListenerService;
import com.senzing.listener.service.scheduling.AbstractSQLSchedulingService;
import com.senzing.listener.service.scheduling.AbstractSchedulingService;
import com.senzing.listener.service.scheduling.PostgreSQLSchedulingService;
import com.senzing.listener.service.scheduling.SQLiteSchedulingService;
import com.senzing.reflect.ReflectionUtilities;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.core.auto.SzAutoCoreEnvironment;
import com.senzing.sdk.core.auto.SzAutoEnvironment;
import com.senzing.text.TextUtilities;
import com.senzing.util.AccessToken;
import com.senzing.util.JsonUtilities;
import com.senzing.sql.*;
import com.senzing.util.LoggingUtilities;
import com.senzing.util.Quantified.Statistic;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import static java.util.Collections.synchronizedCollection;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.time.Duration;
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
import static com.senzing.listener.service.scheduling.AbstractSchedulingService.Stat.*;

/**
 * The data mart replicator command-line class.
 */
public class SzReplicator extends Thread {
    /**
     * The maximum pool wait time.
     */
    private static final long MAX_POOL_WAIT_TIME = 40000L;

    /**
     * The name of the JAR file for command-line execution.
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
     * The {@link DateTimeFormatter} for interpreting the build number as a
     * LocalDateTime instance.
     */
    private static final DateTimeFormatter BUILD_DATE_FORMATTER = DateTimeFormatter.ofPattern(BUILD_DATE_PATTERN)
            .withZone(BUILD_ZONE);

    /**
     * The date-time pattern for the build number.
     */
    private static final String BUILD_NUMBER_PATTERN = "yyyy_MM_dd__HH_mm";

    /**
     * The {@link DateTimeFormatter} for interpreting the build number as a
     * LocalDateTime instance.
     */
    private static final DateTimeFormatter BUILD_NUMBER_FORMATTER = DateTimeFormatter.ofPattern(BUILD_NUMBER_PATTERN);

    /**
     * The {@link String} token used to identify development builds when parsing the
     * version info.
     */
    private static final String DEVELOPMENT_VERSION_TOKEN = "DEVELOPMENT_VERSION";

    /**
     * A constant for the {@link SzEnvironment#destroy()} method.
     */
    private static final Method DESTROY_METHOD;
    static {
        Method method = null;
        try {
            method = SzEnvironment.class.getMethod("destroy");
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
        DESTROY_METHOD = method;
    }

    /**
     * The {@link Object} to synchronize on.
     */
    private final Object monitor = new Object();

    /**
     * The main function for command-line starting.
     *
     * @param args The command-line arguments.
     *
     * @throws Exception If a failure occurs.
     */
    public static void main(String[] args) throws Exception {
        commandLineStart(args, SzReplicator::parseCommandLine, SzReplicator::getUsageString,
                SzReplicator::getVersionString, SzReplicator::build);
    }

    /**
     * Parses the {@link SzReplicator} command line arguments and produces a
     * {@link Map} of {@link CommandLineOption} keys to {@link Object} command line
     * values.
     *
     * @param args                The arguments to parse.
     * @param deprecationWarnings The {@link List} to populate with deprecation
     *                            warnings if any are found in the command line.
     * @return The {@link Map} describing the command-line arguments.
     * @throws CommandLineException If a command-line parsing failure occurs.
     */
    @SuppressWarnings({ "rawtypes" })
    protected static Map<CommandLineOption, Object> parseCommandLine(String[] args, List<DeprecatedOptionWarning> deprecationWarnings) throws CommandLineException {
        Map<CommandLineOption, CommandLineValue> optionValues = CommandLineUtilities.parseCommandLine(
                SzReplicatorOption.class, args, SzReplicatorOption.PARAMETER_PROCESSOR, deprecationWarnings);

        // iterate over the option values and handle them
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("message", "Startup Options");

        StringBuilder sb = new StringBuilder();

        Map<CommandLineOption, Object> result = new LinkedHashMap<>();

        CommandLineUtilities.processCommandLine(optionValues, result, job, sb);

        // log the options
        if (!optionValues.containsKey(HELP) && !optionValues.containsKey(VERSION)) {
            System.out.println("[" + (new Date()) + "] Senzing Data Mart Replicator: " + sb.toString());
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
    @SuppressWarnings("rawtypes")
    private static SzReplicator build(Map<CommandLineOption, Object> options) throws Exception {
        return new SzReplicator(new SzReplicatorOptions(options));
    }

    /**
     * Starts an instance of the server from the command line using the specified
     * arguments and the specified function helpers.
     *
     * @param args           The command-line arguments.
     * @param cmdLineParser  The {@link CommandLineParser} to parse the command line
     *                       arguments.
     * @param usageMessage   The {@link Supplier} for the usage message.
     * @param versionMessage The {@link Supplier} for the version message.
     * @param appBuilder     The {@link CommandLineBuilder} to create the server
     *                       instance from the command line options.
     * @throws Exception If a failure occurs.
     */
    protected static void commandLineStart(String[] args, CommandLineParser cmdLineParser, Supplier<String> usageMessage, Supplier<String> versionMessage, CommandLineBuilder<SzReplicator> appBuilder) throws Exception {
        Map<CommandLineOption, Object> options = null;
        List<DeprecatedOptionWarning> warnings = new LinkedList<>();
        try {
            options = cmdLineParser.parseCommandLine(args, warnings);

            for (DeprecatedOptionWarning warning : warnings) {
                System.out.println(warning);
                System.out.println();
            }

        } catch (CommandLineException e) {
            System.out.println();
            System.out.println(e.getMessage());

            System.err.println();
            System.err.println("Try the " + HELP.getCommandLineFlag() + " option for help.");
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

            synchronized (monitor) {
                while (true) {
                    this.monitor.wait(60000L);
                    ListenerService.State listenerState = this.replicatorService.getState();

                    MessageConsumer.State consumerState = this.messageConsumer.getState();

                    // check if something has interrupted processing
                    if (listenerState != ListenerService.State.AVAILABLE
                            || consumerState != MessageConsumer.State.CONSUMING) {
                        break;
                    }

                    // this.printStatistics();
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
     * Destroys the replicator and causes message consumption to cease.
     */
    public void shutdown() {
        this.messageConsumer.destroy();
        this.replicatorService.destroy();
        try {
            synchronized (this.monitor) {
                this.monitor.notifyAll();
            }
            this.join();

        } catch (InterruptedException e) {
            logWarning(e, "Interrupted while joining against replicator during destroy()");
        } finally {
            if (this.manageEnv) {
                this.environment.destroy();
            }
        }
    }

    /**
     * Gets the {@link Map} of {@link Statistic} keys to {@link Number}
     * values for this instance.
     * 
     * @return The {@link Map} of {@link Statistic} keys to {@link Number}
     *         values for this instance.
     */
    public Map<Statistic, Number> getStatistics() {
        Map<Statistic, Number> stats = new LinkedHashMap<>();
        stats.putAll(this.messageConsumer.getStatistics());
        stats.putAll(this.replicatorService.getStatistics());
        stats.putAll(this.connPool.getStatistics());
        return stats;
    }

    /**
     *
     */
    private void printStatistics() {
        Map<Statistic, Number> stats = this.getStatistics();

        long now = System.nanoTime();
        double elapsed = ((double) (now - this.startTimeNanos)) / 1000000000.0;

        Number completeCount = stats.get(taskGroupCompleteCount);
        long completed = completeCount.longValue();

        MessageConsumer consumer = this.messageConsumer;
        SzReplicatorService service = this.replicatorService;

        System.out.println();
        System.out.println("=====================================================");
        System.out.println("STATISTICS:");
        printStatisticsMap(stats);
    }

    /**
     *
     */
    private static void printStatisticsMap(Map<Statistic, Number> stats) {
        stats.forEach((key, value) -> {
            String units = key.getUnits();
            System.out.println("  " + key.getName() + ": " + value + ((units != null) ? " " + units : ""));
        });
    }

    /**
     * Returns a formatted string describing the version details of the API Server.
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
        pw.println("[ " + JAR_FILE_NAME + " version " + BuildInfo.MAVEN_VERSION + " ]");
    }

    /**
     * Prints the Senzing version information to the specified {@link PrintWriter}.
     *
     * @param pw The {@link PrintWriter} to print the version information to.
     */
    protected static void printSenzingVersions(PrintWriter pw) {
        SzEnvironment env = SzCoreEnvironment.getActiveInstance();
        boolean manageEnv = false;
        try {
            // check if we need to create an environment
            if (env == null) {
                env = SzCoreEnvironment.newBuilder().build();
                manageEnv = true;
            }

            String jsonText = env.getProduct().getVersion();
            JsonObject jsonObj = JsonUtilities.parseJsonObject(jsonText);

            String nativeApiVersion = JsonUtilities.getString(jsonObj, "VERSION");
            String buildVersion = JsonUtilities.getString(jsonObj, "BUILD_VERSION");
            String buildNumber = JsonUtilities.getString(jsonObj, "BUILD_NUMBER");

            Date buildDate = null;
            if (buildNumber != null && buildNumber.length() > 0 && buildNumber.indexOf(DEVELOPMENT_VERSION_TOKEN) < 0) {
                LocalDateTime localDateTime = LocalDateTime.parse(buildNumber, BUILD_NUMBER_FORMATTER);
                ZonedDateTime zonedDateTime = localDateTime.atZone(BUILD_ZONE);
                buildDate = Date.from(zonedDateTime.toInstant());

            } else {
                buildDate = new Date();
            }

            String formattedBuildDate = BUILD_DATE_FORMATTER.format(Instant.ofEpochMilli(buildDate.getTime()));

            JsonObject compatVersion = JsonUtilities.getJsonObject(jsonObj, "COMPATIBILITY_VERSION");

            String configCompatVersion = JsonUtilities.getString(compatVersion, "CONFIG_VERSION");

            pw.println(" - Senzing Replicator Version   : " + BuildInfo.MAVEN_VERSION);
            pw.println(" - Senzing Native API Version   : " + nativeApiVersion);
            pw.println(" - Senzing Native Build Version : " + buildVersion);
            pw.println(" - Senzing Native Build Number  : " + buildNumber);
            pw.println(" - Senzing Native Build Date    : " + formattedBuildDate);
            pw.println(" - Config Compatibility Version : " + configCompatVersion);
            pw.flush();

        } catch (SzException e) {
            pw.println("Failure in getting version: " + e.getMessage());
            pw.println(LoggingUtilities.formatStackTrace(e.getStackTrace()));
            pw.flush();

        } finally {
            if (manageEnv) {
                env.destroy();
            }
        }
    }

    /**
     * Prints the introduction to the usage message to the specified
     * {@link PrintWriter}.
     *
     * @param pw The {@link PrintWriter} to write the usage introduction to.
     */
    protected static void printUsageIntro(PrintWriter pw) {
        pw.println(multilineFormat("java -jar " + JAR_FILE_NAME + " <options>", "", "<options> includes: "));
    }

    /**
     * Prints the usage for the standard options to the specified
     * {@link PrintWriter}.
     *
     * @param pw The {@link PrintWriter} to write the standard options usage.
     */
    protected static void printStandardOptionsUsage(PrintWriter pw) {
        pw.println(multilineFormat("[ Standard Options ]", "   --help",
                "        Should be the first and only option if provided.  Causes this help",
                "        message to be displayed.",
                "        NOTE: If this option is provided, the replicator will not start.", "", "   --version",
                "        Should be the first and only option if provided.  Causes the version",
                "        of the G2 REST API Server to be displayed.",
                "        NOTE: If this option is provided, the replicator will not start.", "",
                "   --concurrency <thread-count>",
                "        Sets the number of threads available for performing Senzing API",
                "        operations (i.e.: the number of engine threads).  The number of",
                "        threads for consuming messages and handling tasks is scaled based",
                "        on the engine concurrency.  If not specified, then this defaults to "
                            + DEFAULT_CORE_CONCURRENCY + ".",
                "        --> VIA ENVIRONMENT: " + CORE_CONCURRENCY.getEnvironmentVariable(), "",
                "   --module-name <module-name>",
                "        The module name to initialize with.  If not specified, then the module",
                "        name defaults to \"" + DEFAULT_INSTANCE_NAME + "\".",
                "        --> VIA ENVIRONMENT: " + CORE_INSTANCE_NAME.getEnvironmentVariable(), "", "   --verbose [true|false]",
                "        Also -verbose.  If specified then initialize in verbose mode.  The",
                "        true/false parameter is optional, if not specified then true is assumed.",
                "        If specified as false then it is the same as omitting the option with",
                "        the exception that omission falls back to the environment variable",
                "        setting whereas an explicit false overrides any environment variable.",
                "   --init-json <json-init-text>", "        The JSON text to use for Senzing initialization.",
                "        *** SECURITY WARNING: If the JSON text contains a password",
                "        then it may be visible to other users via process monitoring.",
                "        EXAMPLE: -initJson \"{\"PIPELINE\":{ ... }}\"",
                "        --> VIA ENVIRONMENT: " + CORE_SETTINGS.getEnvironmentVariable()));
    }

    /**
     * Prints the info-queue options usage to the specified {@link PrintWriter}.
     *
     * @param pw The {@link PrintWriter} to write the info-queue options usage.
     */
    protected static void printInfoQueueOptionsUsage(PrintWriter pw) {
        pw.println(multilineFormat("[ Asynchronous Info Queue Options ]",
                "   The following options pertain to configuring the message queue from which to",
                "   receive the \"info\" messages.  Exactly one such queue must be configured.", "",
                "   --database-info-queue [true|false]",
                "        Configures the data mart replicator to leverage the configured database",
                "        to obtain the INFO messages via the sz_message_queue table.  If using a",
                "        SQLite database you should ensure messages are not being written to the",
                "        queue by another process at the same time they are being consumed since",
                "        SQLite does not support concurrent writes from multiple connections",
                "        --> VIA ENVIRONMENT: " + DATABASE_INFO_QUEUE.getEnvironmentVariable(), "",
                "   --sqs-info-uri <uri>", "        Specifies an Amazon SQS queue URL as the info queue.",
                "        --> VIA ENVIRONMENT: " + SQS_INFO_URI.getEnvironmentVariable(), "",
                "   --rabbit-info-uri <rabbitmq-connection-uri>",
                "        Used to specify the hostname for connecting to RabbitMQ as part of",
                "        specifying a RabbitMQ info queue.",
                "        --> VIA ENVIRONMENT: " + RABBITMQ_URI.getEnvironmentVariable(),
                "                             " + RABBITMQ_URI.getEnvironmentFallbacks().iterator().next()
                        + " (fallback)",
                "", "   --rabbit-info-queue <queue name>",
                "        Used to specify the name of the RabbitMQ queue from which to pull the",
                "        info messages.",
                "        --> VIA ENVIRONMENT: " + RABBITMQ_INFO_QUEUE.getEnvironmentVariable()));
    }

    /**
     * Prints the data-mart database connectivity options usage to the specified
     * {@link PrintWriter}.
     *
     * @param pw The {@link PrintWriter} to write the data-mart database
     *           connectivity options usage.
     */
    protected static void printDatabaseOptionsUsage(PrintWriter pw) {
        pw.println(multilineFormat("[ Data Mart Database Connectivity Options ]",
                "   The following options pertain to configuring the connection to the data-mart",
                "   database.  Exactly one such database must be configured.", 
                "", 
                "   --sqlite-database-file <url>",
                "        Specifies an SQLite database file to open (or create) to use as the",
                "        data-mart database.  NOTE: SQLite may be used for testing, but because",
                "        only one connection may be made, it will not scale for production use.",
                "        --> VIA ENVIRONMENT: " + DATABASE_URI.getEnvironmentVariable(),
                ""));
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
     * The {@link ConnectionProvider} that is backed by the {@link ConnectionPool}.
     */
    private ConnectionProvider connProvider;

    /**
     * The unique name used to bind the {@link ConnectionProvider} in the
     * {@link ConnectionProvider#REGISTRY}.
     */
    private String connProviderName;

    /**
     * The name under which to register the message queue if using a database queue.
     */
    private String queueRegistryName = null;

    /**
     * The name under which to register the message queue if using a database queue.
     */
    private SQLConsumer.MessageQueue sqlMessageQueue = null;

    /**
     * The {@link SzEnvironment} to use.
     */
    private SzEnvironment environment = null;

    /**
     * The proxied {@link SzEnvironment} to prevent calling of
     * {@link #destroy()}.
     */
    private SzEnvironment proxyEnvironment = null;

    /**
     * The flag indicating if this instance should manage the {@link SzEnvironment}.
     */
    private boolean manageEnv = false;

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
     * Creates a new instance of {@link SzAutoCoreEnvironment} using 
     * the specified options.
     * 
     * @param options The {@link SzReplicatorOptions} to use.
     * @return The {@link SzAutoCoreEnvironment} that was created using the
     *         specified options.
     * 
     * @throws IllegalStateException If there is already an active instance of
     *                               {@link com.senzing.sdk.core.SzCoreEnvironment}.
     */
    protected static SzAutoCoreEnvironment createSzAutoCoreEnvironment(
        SzReplicatorOptions options)
        throws IllegalStateException
    {
        String settings = JsonUtilities.toJsonText(options.getCoreSettings());

        String instanceName = options.getCoreInstanceName();
        
        boolean verbose = (options.getCoreLogLevel() != 0);
        
        int concurrency = options.getCoreConcurrency();
     
        long refreshSeconds = options.getRefreshConfigSeconds();
        Duration duration = (refreshSeconds < 0) 
            ? null : Duration.ofSeconds(refreshSeconds);

        return SzAutoCoreEnvironment.newAutoBuilder()
                .concurrency(concurrency)
                .configRefreshPeriod(duration)
                .settings(settings)
                .instanceName(instanceName)
                .verboseLogging(verbose)
                .build();
    }

    /**
     * Constructs an instance of {@link SzReplicator} with the specified
     * {@link SzReplicatorOptions} instance.  The server will be started
     * upon construction.
     *
     * <b>NOTE:</b> This will initialize the Senzing Core SDK via
     * {@link SzAutoCoreEnvironment} and only one active instance of
     * {@link com.senzing.sdk.core.SzCoreEnvironment} is allowed in a
     * process at any given time.
     * 
     * @param options The {@link SzReplicatorOptions} instance with which to
     *                construct the API server instance.
     * 
     * @throws IllegalStateException If another instance of Senzing Core SDK
     *                               is already actively initialized.
     * 
     * @throws Exception If a failure occurs.
     */
    public SzReplicator(SzReplicatorOptions options) 
        throws Exception 
    {
        this(options, true);
    }

    /**
     * Constructs an instance of {@link SzReplicator} with the specified
     * {@link SzReplicatorOptions} instance, optionally {@linkplain 
     * #start() starting} processing upon construction.
     *
     * @param options The {@link SzReplicatorOptions} instance with which to
     *                construct the API server instance.
     * 
     * @param startProcessing <code>true</code> if processing should be started
     *                        upon construction, otherwise <code>false</code>.
     * 
     * @throws Exception If a failure occurs.
     */
    public SzReplicator(SzReplicatorOptions options, boolean startProcessing) 
        throws Exception 
    {
        this(createSzAutoCoreEnvironment(options),
             true,
             options,
             startProcessing);
    }

    /**
     * Constructs an instance of {@link SzReplicator} with the specified
     * {@link SzEnvironment} and {@link SzReplicatorOptions} instance.
     * The constructed instance will <b>not</b> manage the specified
     * {@link SzEnvironment} in that it will <b>not</b> attempt {@linkplain
     * SzEnvironment#destroy() destroy} it upon destruction of this instance.
     *
     * <p>
     * <b>NOTE:</b> Any of the {@linkplain SzReplicatorOptions options}
     * specified pertaining to the creation of an {@link SzAutoCoreEnvironment}
     * will be ignored.
     * 
     * @param environment The {@link SzEnvironment} to use.
     * 
     * @param options     The {@link SzReplicatorOptions} instance with which to
     *                    construct the API server instance.
     * 
     * @param startProcessing <code>true</code> if processing should be started
     *                        upon construction, otherwise <code>false</code>.
     * 
     * @throws Exception If a failure occurs.
     */
    public SzReplicator(SzEnvironment       environment,
                        SzReplicatorOptions options,
                        boolean             startProcessing)
        throws Exception 
    {
        this(environment, false, options, startProcessing);
    }

    /**
     * Constructs an instance of {@link SzReplicator} with the specified
     * {@link SzReplicatorOptions} instance.
     *
     * @param environment The {@link SzEnvironment} to use.
     * 
     * @param manageEnv <code>true</code> if this instance should destroy the
     *                  environment when done, otherwise <code>false</code>.
     * 
     * @param options     The {@link SzReplicatorOptions} instance with which to
     *                    construct the API server instance.
     * 
     * @param startProcessing <code>true</code> if processing should be started
     *                        upon construction, otherwise <code>false</code>.
     * 
     * @throws Exception If a failure occurs.
     */
    protected SzReplicator(SzEnvironment        environment,
                           boolean              manageEnv,
                           SzReplicatorOptions  options,
                           boolean              startProcessing)
        throws Exception 
    {
        // get the concurrency
        if (environment instanceof SzAutoEnvironment) {
            SzAutoEnvironment autoEnv = (SzAutoEnvironment) environment;
            this.concurrency = autoEnv.getConcurrency();
        } else {
            this.concurrency = options.getCoreConcurrency();
        }

        final int consumerConcurrency = this.concurrency * 2;
        final int scheduleConcurrency = this.concurrency * 2;
        final int poolSize = this.concurrency;
        final int maxPoolSize = poolSize * 3;

        // set the environment
        this.environment = environment;
        this.manageEnv   = manageEnv;

        // proxy the environment
        this.proxyEnvironment = (SzEnvironment)
            ReflectionUtilities.restrictedProxy(this.environment, DESTROY_METHOD);
        
        // declare the scheduling service class (determine based on database type)
        String schedulingServiceClassName = null;

        // get the database URI
        ConnectionUri databaseUri = options.getDatabaseUri();
        
        if (databaseUri instanceof SqliteUri) {
            SqliteUri sqliteUri = (SqliteUri) databaseUri;
            File dbFile = sqliteUri.getFile();

            this.connector = new SQLiteConnector(dbFile);

            this.connPool = new ConnectionPool(this.connector, poolSize, maxPoolSize);

            schedulingServiceClassName = SQLiteSchedulingService.class.getName();

        } else if (databaseUri instanceof PostgreSqlUri) {
            PostgreSqlUri postgreSqlUri = (PostgreSqlUri) databaseUri;

            this.connector = new PostgreSqlConnector(postgreSqlUri.getHost(),
                                                     postgreSqlUri.getPort(),
                                                     postgreSqlUri.getDatabase(),
                                                     postgreSqlUri.getHost(),
                                                     postgreSqlUri.getPassword());

            this.connPool = new ConnectionPool(this.connector, 
                                               TransactionIsolation.READ_COMMITTED,
                                               poolSize,
                                               maxPoolSize);

            schedulingServiceClassName = PostgreSQLSchedulingService.class.getName();
        }

        this.connProvider = new PoolConnectionProvider(this.connPool, MAX_POOL_WAIT_TIME);

        this.connProviderName = TextUtilities.randomAlphanumericText(30);

        this.connProviderToken = ConnectionProvider.REGISTRY.bind(this.connProviderName, this.connProvider);

        // get the processing rate
        ProcessingRate processingRate = options.getProcessingRate();

        // handle the scheduling service config
        JsonObjectBuilder schedulingJOB = processingRate.addSchedulingServiceOptions(null);
        schedulingJOB.add(AbstractSchedulingService.CONCURRENCY_KEY, scheduleConcurrency);
        schedulingJOB.add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, this.connProviderName);

        // handle the replicator config
        JsonObjectBuilder replicatorJOB = processingRate.addReplicatorServiceOptions(null);
        replicatorJOB.add(SzReplicatorService.SCHEDULING_SERVICE_CLASS_KEY, schedulingServiceClassName);

        replicatorJOB.add(SzReplicatorService.SCHEDULING_SERVICE_CONFIG_KEY, schedulingJOB);

        replicatorJOB.add(SzReplicatorService.CONNECTION_PROVIDER_KEY, this.connProviderName);

        this.replicatorService = new SzReplicatorService(this.proxyEnvironment);
        this.replicatorService.init(replicatorJOB.build());

        // build the message consumer
        RabbitMqUri rabbitMqUri     = options.getRabbitMqUri();
        SqsUri      sqsUri          = options.getSqsInfoUri();
        Boolean     databaseQueue   = options.isUsingDatabaseQueue();
        
        JsonObjectBuilder consumerJOB = Json.createObjectBuilder();
        if (Boolean.TRUE.equals(databaseQueue))  {
            this.queueRegistryName = TextUtilities.randomAlphanumericText(25);
            consumerJOB.add(SQLConsumer.CONNECTION_PROVIDER_KEY, this.connProviderName);
            consumerJOB.add(SQLConsumer.QUEUE_REGISTRY_NAME_KEY, this.queueRegistryName);
            this.messageConsumer = new SQLConsumer();

        } else if (rabbitMqUri != null) {
            String queueName = options.getRabbitMqInfoQueue();
            if (queueName == null) {
                throw new IllegalArgumentException(
                    "The RabbitMQ MQ must be specified if the RabbitMQ URI is provided.");
            }
            consumerJOB.add(RabbitMQConsumer.CONCURRENCY_KEY, consumerConcurrency);
            consumerJOB.add(RabbitMQConsumer.MQ_HOST_KEY, rabbitMqUri.getHost());
            consumerJOB.add(RabbitMQConsumer.MQ_USER_KEY, rabbitMqUri.getUser());
            consumerJOB.add(RabbitMQConsumer.MQ_PASSWORD_KEY, rabbitMqUri.getPassword());
            consumerJOB.add(RabbitMQConsumer.MQ_QUEUE_KEY, queueName);
            
            // check if we have the port parameter
            if (rabbitMqUri.hasPort()) {
                consumerJOB.add(RabbitMQConsumer.MQ_PORT_KEY, rabbitMqUri.getPort());
            }

            // check if we have the virtual host parameter
            if (rabbitMqUri.getVirtualHost() != null) {
                consumerJOB.add(RabbitMQConsumer.MQ_VIRTUAL_HOST_KEY, rabbitMqUri.getVirtualHost());
            }

            this.messageConsumer = new RabbitMQConsumer();

        } else if (sqsUri != null) {
            consumerJOB.add(SQSConsumer.CONCURRENCY_KEY, consumerConcurrency);

            // build an SQS message consumer
            consumerJOB.add(SQSConsumer.SQS_URL_KEY, sqsUri.toString());
            this.messageConsumer = new SQSConsumer();

        } else {
            throw new IllegalStateException("Missing INFO queue option: " + options);
        }
        this.messageConsumer.init(consumerJOB.build());
        if (this.queueRegistryName != null) {
            this.sqlMessageQueue = SQLConsumer.MESSAGE_QUEUE_REGISTRY.lookup(this.queueRegistryName);
        }
    }

    /**
     * Gets the {@link SzReplicationProvider} for this instance.
     * 
     * @return The {@link SzReplicationProvider} for this instance.
     */
    public SzReplicationProvider getReplicationProvider() {
        return this.replicatorService.getReplicationProvider();
    }

    /**
     * Gets the {@link SQLConsumer.MessageQueue} instance backing the underlying
     * {@link SQLConsumer} if database message queue is being employed rather than
     * RabbitMQ or Amazon SQS. This returns <code>null</code> if this instance is
     * configured to use a RabbitMQ or Amazon SQS queue.
     * 
     * @return The {@link SQLConsumer.MessageQueue} instance backing the underlying
     *         {@link SQLConsumer} if database message queue is being employed, or
     *         <code>null</code> if the configured message queue for this instance
     *         is RabbitMQ or Amazon SQS.
     */
    public SQLConsumer.MessageQueue getDatabaseMessageQueue() {
        return this.sqlMessageQueue;
    }

}
