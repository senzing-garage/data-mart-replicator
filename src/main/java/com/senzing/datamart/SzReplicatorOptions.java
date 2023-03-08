package com.senzing.datamart;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.File;
import java.util.*;

import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.datamart.SzReplicatorOption.*;
import static com.senzing.util.JsonUtilities.*;

/**
 * Describes the options to be set when constructing an instance of
 * {@link SzReplicator}.
 */
public class SzReplicatorOptions {
  private JsonObject  jsonInit                  = null;
  private int         concurrency               = DEFAULT_CONCURRENCY;
  private String      moduleName                = DEFAULT_MODULE_NAME;
  private boolean     verbose                   = false;
  private String      rabbitInfoUser            = null;
  private String      rabbitInfoPassword        = null;
  private String      rabbitInfoHost            = null;
  private Integer     rabbitInfoPort            = null;
  private String      rabbitInfoVHost           = null;
  private String      rabbitInfoQueue           = null;
  private String      sqsInfoUrl                = null;
  private File        sqliteDatabaseFile        = null;
  private String      postgreSqlHost            = null;
  private Integer     postgreSqlPort            = null;
  private String      postgreSqlDatabase        = null;
  private String      postgreSqlUser            = null;
  private String      postgreSqlPassword        = null;

  /**
   * Constructs with the native Senzing JSON initialization parameters as a
   * {@link JsonObject}.
   *
   * @param jsonInit The JSON initialization parameters.
   */
  public SzReplicatorOptions(JsonObject jsonInit) {
    Objects.requireNonNull(jsonInit,
                           "JSON init parameters cannot be null");
    this.jsonInit = jsonInit;
  }

  /**
   * Constructs with the native Senzing JSON initialization parameters as JSON
   * text.
   *
   * @param jsonInitText The JSON initialization parameters as JSON text.
   */
  public SzReplicatorOptions(String jsonInitText) {
    this(JsonUtilities.parseJsonObject(jsonInitText));
  }

  /**
   * Returns the {@link JsonObject} describing the initialization parameters
   * for the Senzing engine.
   *
   * @return The {@link JsonObject} describing the initialization parameters
   *         for the Senzing engine.
   */
  public JsonObject getJsonInitParameters() {
    return this.jsonInit;
  }

  /**
   * Gets the module name to initialize with.  If <code>null</code> is returned
   * then {@link SzReplicatorConstants#DEFAULT_MODULE_NAME} is used.
   *
   * @return The module name to initialize with, or <code>null</code> is
   *         returned then {@link SzReplicatorConstants#DEFAULT_MODULE_NAME}
   *         is used.
   */
  public String getModuleName() {
    return this.moduleName;
  }

  /**
   * Sets the module name to initialize with.  Set to <code>null</code> if the
   * default value of {@link SzReplicatorConstants#DEFAULT_MODULE_NAME} is to be
   * used.
   *
   * @param moduleName The module name to bind to, or <code>null</code> then the
   *                   {@link SzReplicatorConstants#DEFAULT_MODULE_NAME} is
   *                   used.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setModuleName(String moduleName) {
    this.moduleName = moduleName;
    return this;
  }

  /**
   * Sets the verbosity to initialize with.
   *
   * @param verbose The verbosity to initialize with (either <code>true</code>
   *                for verbose and <code>false</code> for not verbose).
   */
  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  /**
   * Checks if we should initialize the Senzing engine as verbose or not.
   *
   * @return <code>true</code> if we should initialize as verbose, and
   *         <code>false</code> if not.
   */
  public boolean isVerbose() {
    return this.verbose;
  }

  /**
   * Gets the number of threads that the server will create for the engine.
   * If the value has not {@linkplain #setConcurrency(Integer) explicitly set}
   * then {@link SzReplicatorConstants#DEFAULT_CONCURRENCY} is returned.
   *
   * @return The number of threads that the server will create for the engine.
   */
  public int getConcurrency() {
    return this.concurrency;
  }

  /**
   * Sets the number of threads that the server will create for the engine.
   * Set to <code>null</code> to use the {@linkplain
   * SzReplicatorConstants#DEFAULT_CONCURRENCY default number of threads}.
   *
   * @param concurrency The number of threads to create for the engine, or
   *                    <code>null</code> for the default number of threads.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setConcurrency(Integer concurrency) {
    this.concurrency = (concurrency != null)
        ? concurrency : DEFAULT_CONCURRENCY;
    return this;
  }

  /**
   * Returns the RabbitMQ user for the "info" queue.
   *
   * @return The RabbitMQ user for the "info" queue.
   */
  public String getRabbitInfoUser() {
    return this.rabbitInfoUser;
  }

  /**
   * Sets the RabbitMQ user for the "info" queue.
   *
   * @param user The RabbitMQ user for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setRabbitInfoUser(String user) {
    this.rabbitInfoUser = user;
    return this;
  }

  /**
   * Returns the RabbitMQ password for the "info" queue.
   *
   * @return The RabbitMQ password for the "info" queue.
   */
  public String getRabbitInfoPassword() {
    return this.rabbitInfoPassword;
  }

  /**
   * Sets the RabbitMQ password for the "info" queue.
   *
   * @param password The RabbitMQ password for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setRabbitInfoPassword(String password) {
    this.rabbitInfoPassword = password;
    return this;
  }

  /**
   * Returns the RabbitMQ host for the "info" queue.
   *
   * @return The RabbitMQ host for the "info" queue.
   */
  public String getRabbitInfoHost() {
    return this.rabbitInfoHost;
  }

  /**
   * Sets the RabbitMQ host for the "info" queue.
   *
   * @param host The RabbitMQ host for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setRabbitInfoHost(String host) {
    this.rabbitInfoHost = host;
    return this;
  }

  /**
   * Returns the RabbitMQ port for the "info" queue.
   *
   * @return The RabbitMQ port for the "info" queue.
   */
  public Integer getRabbitInfoPort() {
    return this.rabbitInfoPort;
  }

  /**
   * Sets the RabbitMQ port for the "info" queue.
   *
   * @param port The RabbitMQ port for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setRabbitInfoPort(Integer port) {
    this.rabbitInfoPort = port;
    return this;
  }

  /**
   * Returns the RabbitMQ virtual host for the "info" queue.
   *
   * @return The RabbitMQ virtual host for the "info" queue.
   */
  public String getRabbitInfoVirtualHost() {
    return this.rabbitInfoVHost;
  }

  /**
   * Sets the RabbitMQ virtual host for the "info" queue.
   *
   * @param virtualHost The RabbitMQ virtual host for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setRabbitInfoVirtualHost(String virtualHost) {
    this.rabbitInfoVHost = virtualHost;
    return this;
  }

  /**
   * Returns the RabbitMQ queue name for the "info" queue.
   *
   * @return The RabbitMQ routing key for the "info" queue.
   */
  public String getRabbitInfoQueue() {
    return this.rabbitInfoQueue;
  }

  /**
   * Sets the RabbitMQ queue name for the "info" queue.
   *
   * @param queueName The RabbitMQ queue name for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setRabbitInfoQueue(String queueName) {
    this.rabbitInfoQueue = queueName;
    return this;
  }

  /**
   * Returns the SQS URL for the "info" queue.
   *
   * @return The SQS URL for the "info" queue.
   */
  public String getSqsInfoUrl() {
    return sqsInfoUrl;
  }

  /**
   * Sets the SQS URL for the "info" queue.
   *
   * @param url The SQS URL for the "info" queue.
   *
   * @return A reference to this instance.
   */
  public SzReplicatorOptions setSqsInfoUrl(String url) {
    this.sqsInfoUrl = url;
    return this;
  }

  /**
   * Gets the database file for the Sqlite data mart database.
   *
   * @return The database file for connecting to the Sqlite database.
   *
   * @see SzReplicatorOption#SQLITE_DATABASE_FILE
   */
  public File getSqliteDatabaseFile() {
    return this.sqliteDatabaseFile;
  }

  /**
   * Sets the database file for the Sqlite data mart database.
   *
   * @param file The database file for connecting to the Sqlite database.
   *
   * @see SzReplicatorOption#SQLITE_DATABASE_FILE
   */
  public void setSqliteDatabaseFile(File file) {
    this.sqliteDatabaseFile = file;
  }

  /**
   * Gets the database host for the PostgreSql data mart database.
   *
   * @return The database host for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_HOST
   */
  public String getPostgreSqlHost() {
    return this.postgreSqlHost;
  }

  /**
   * Sets the database host for the PostgreSql data mart database.
   *
   * @param host The database port for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_HOST
   */
  public void setPostgreSqlHost(String host) {
    this.postgreSqlHost = host;
  }

  /**
   * Gets the database port for the PostgreSql data mart database.
   *
   * @return The database port for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_PORT
   */
  public Integer getPostgreSqlPort() {
    return this.postgreSqlPort;
  }

  /**
   * Sets the database port for the PostgreSql data mart database.
   *
   * @param port The database port for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_PORT
   */
  public void setPostgreSqlPort(Integer port) {
    this.postgreSqlPort = port;
  }

  /**
   * Gets the database name for the PostgreSql data mart database.
   *
   * @return The database name for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_DATABASE
   */
  public String getPostgreSqlDatabase() {
    return this.postgreSqlDatabase;
  }

  /**
   * Sets the database name for the PostgreSql data mart database.
   *
   * @param database The database name for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_DATABASE
   */
  public void setPostgreSqlDatabase(String database) {
    this.postgreSqlDatabase = database;
  }

  /**
   * Gets the user name for the PostgreSql data mart database.
   *
   * @return The user name for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_USER
   */
  public String getPostgreSqlUser() {
    return this.postgreSqlUser;
  }

  /**
   * Sets the user name for the PostgreSql data mart database.
   *
   * @param user The user name for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_USER
   */
  public void setPostgreSqlUser(String user) {
    this.postgreSqlUser = user;
  }

  /**
   * Gets the password for the PostgreSql data mart database.
   *
   * @return The password for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_PASSWORD
   */
  public String getPostgreSqlPassword() {
    return this.postgreSqlPassword;
  }

  /**
   * Sets the password for the PostgreSql data mart database.
   *
   * @param password The password for connecting to the database.
   *
   * @see SzReplicatorOption#POSTGRESQL_PASSWORD
   */
  public void setPostgreSqlPassword(String password) {
    this.postgreSqlPassword = password;
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
   * Converts this instance to JSON.
   *
   * @param builder The {@link JsonObjectBuilder} to which to add the JSON
   *                properties, or <code>null</code> if a new {@link
   *                JsonObjectBuilder} should be created.
   * @return The specified {@link JsonObjectBuilder}.
   */
  public JsonObjectBuilder buildJson(JsonObjectBuilder builder) {
    if (builder == null) builder = Json.createObjectBuilder();
    builder.add("initJson", this.getJsonInitParameters());
    builder.add("verbose", this.isVerbose());
    builder.add("concurrency", this.getConcurrency());
    builder.add("moduleName", this.getModuleName());
    builder.add("rabbitInfoUser", this.getRabbitInfoUser());
    builder.add("rabbitInfoPassword", this.getRabbitInfoPassword());
    builder.add("rabbitInfoHost", this.getRabbitInfoHost());
    builder.add("rabbitInfoPort", this.getRabbitInfoPort());
    builder.add("rabbitInfoVirtualHost", this.getRabbitInfoVirtualHost());
    builder.add("rabbitInfoQueue", this.getRabbitInfoQueue());
    builder.add("sqsInfoUrl", this.getSqsInfoUrl());
    builder.add("sqliteDatabaseFile", "" + this.getSqliteDatabaseFile());
    builder.add("postgreSqlHost", this.getPostgreSqlHost());
    builder.add("postgreSqlPort", this.getPostgreSqlPort());
    builder.add("postgreSqlDatabase", this.getPostgreSqlDatabase());
    builder.add("postgreSqlUser", this.getPostgreSqlUser());
    builder.add("postgreSqlPassword", this.getPostgreSqlPassword());
    return builder;
  }

  /**
   * Parses the specified JSON text as an instance of {@link
   * SzReplicatorOptions}.
   *
   * @param jsonText The JSON text describing the {@link SzReplicatorOptions}.
   *
   * @return The {@link SzReplicatorOptions} created from the specified
   *         JSON text.
   */
  public static SzReplicatorOptions parse(String jsonText) {
    JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);
    return parse(jsonObject);
  }

  /**
   * Parses the specified {@link JsonObject} as an instance of {@link
   * SzReplicatorOptions}.
   *
   * @param jsonObject The {@link JsonObject} describing the {@link
   *                   SzReplicatorOptions}.
   *
   * @return The {@link SzReplicatorOptions} created from the specified
   *         {@link JsonObject}.
   */
  public static SzReplicatorOptions parse(JsonObject jsonObject) {
    final JsonObject obj = jsonObject;
    SzReplicatorOptions opts
        = new SzReplicatorOptions(getJsonObject(obj, "initJson"));

    opts.setConcurrency(getInteger(obj, "concurrency"));
    opts.setModuleName(getString(obj, "moduleName"));
    opts.setRabbitInfoUser(getString(obj, "rabbitInfoUser"));
    opts.setRabbitInfoPassword(getString(obj, "rabbitInfoPassword"));
    opts.setRabbitInfoHost(getString(obj, "rabbitInfoHost"));
    opts.setRabbitInfoPort(getInteger(obj, "rabbitInfoPort"));
    opts.setRabbitInfoVirtualHost(getString(obj, "rabbitInfoVirtualHost"));
    opts.setRabbitInfoQueue(getString(obj, "rabbitInfoQueue"));
    opts.setSqsInfoUrl(getString(obj, "sqsInfoUrl"));
    opts.setSqliteDatabaseFile(new File(getString(obj,
                                                  "sqliteDatabaseFile")));
    opts.setPostgreSqlHost(getString(obj, "postgreSqlHost"));
    opts.setPostgreSqlPort(getInteger(obj, "postgreSqlPort"));
    opts.setPostgreSqlDatabase(getString(obj, "postgreSqlDatabase"));
    opts.setPostgreSqlUser(getString(obj, "postgreSqlUser"));
    opts.setPostgreSqlPassword(getString(obj, "postgreSqlPassword"));
    return opts;
  }

  /**
   * Creates a {@link Map} of {@link CommandLineOption} keys to {@link Object}
   * values for initializing an {@link SzReplicator} instance.
   *
   * @return The {@link Map} of {@link CommandLineOption} keys to {@link Object}
   *         values for initializing an {@link SzReplicator} instance.
   */
  protected Map<CommandLineOption, Object> buildOptionsMap() {
    Map<CommandLineOption, Object> map = new HashMap<>();
    put(map, MODULE_NAME,               this.getModuleName());
    put(map, INIT_JSON,                 this.getJsonInitParameters());
    put(map, VERBOSE,                   this.isVerbose());
    put(map, CONCURRENCY,               this.getConcurrency());
    put(map, RABBIT_INFO_USER,          this.getRabbitInfoUser());
    put(map, RABBIT_INFO_PASSWORD,      this.getRabbitInfoPassword());
    put(map, RABBIT_INFO_HOST,          this.getRabbitInfoHost());
    put(map, RABBIT_INFO_PORT,          this.getRabbitInfoPort());
    put(map, RABBIT_INFO_VIRTUAL_HOST,  this.getRabbitInfoVirtualHost());
    put(map, RABBIT_INFO_QUEUE,         this.getRabbitInfoQueue());
    put(map, SQS_INFO_URL,              this.getSqsInfoUrl());
    put(map, SQLITE_DATABASE_FILE,      this.getSqliteDatabaseFile());
    put(map, POSTGRESQL_HOST,           this.getPostgreSqlHost());
    put(map, POSTGRESQL_PORT,           this.getPostgreSqlPort());
    put(map, POSTGRESQL_DATABASE,       this.getPostgreSqlDatabase());
    put(map, POSTGRESQL_USER,           this.getPostgreSqlUser());
    put(map, POSTGRESQL_PASSWORD,       this.getPostgreSqlPassword());
    return map;
  }

  /**
   * Utility method to only put non-null values in the specified {@link Map}
   * with the specified {@link SzReplicatorOption} key and {@link Object} value.
   *
   * @param map The {@link Map} to put the key-value pair into.
   * @param option The {@link SzReplicatorOption} key.
   * @param value The {@link Object} value.
   */
  private static void put(Map<CommandLineOption, Object>  map,
                          SzReplicatorOption              option,
                          Object                          value)
  {
    if (value != null) {
      map.put(option, value);
    }
  }
}
