package com.senzing.datamart;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.util.JsonUtils;
import com.senzing.reflect.PropertyReflector;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.datamart.SzReplicatorOption.*;

/**
 * Describes the options to be set when constructing an instance of
 * {@link SzReplicator}.
 */
public class SzReplicatorOptions {
  private JsonObject  jsonInit                  = null;
  private int         concurrency               = DEFAULT_CONCURRENCY;
  private String      moduleName                = DEFAULT_MODULE_NAME;
  private String      rabbitInfoUser            = null;
  private String      rabbitInfoPassword        = null;
  private String      rabbitInfoHost            = null;
  private Integer     rabbitInfoPort            = null;
  private String      rabbitInfoVHost           = null;
  private String      rabbitInfoQueue           = null;
  private String      sqsInfoUrl                = null;

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
    this(JsonUtils.parseJsonObject(jsonInitText));
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
   *
   */
  public JsonObject toJson() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    this.buildJson(builder);
    return builder.build();
  }

  /**
   *
   */
  public JsonObjectBuilder buildJson(JsonObjectBuilder builder) {
    PROPERTY_REFLECTOR.getAccessors().forEach((propName, method) -> {
      try {
        Object propValue = method.invoke(this);
        switch (method.getReturnType().getName()) {
          case "javax.json.JsonObject":
            if (propValue != null) {
              JsonObject jsonObj = (JsonObject) propValue;
              builder.add(propName, Json.createObjectBuilder(jsonObj));
            }
            return;

          case "Integer":
          case "int":
            addInt(builder, propName, (Integer) propValue);
            return;

          case "String":
            addString(builder, propName, (String) propValue);
            return;

          default:
            throw new IllegalStateException(
                "Unhandled property type: " + method.getReturnType());
        }
      } catch (IllegalAccessException|InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    });
    return builder;
  }

  /**
   * Conditionally adds the specified {@link String} to the specified
   * {@link JsonObjectBuilder} with the specified key if the specified
   * {@link String} value is <b>not</b> <code>null</code>.
   *
   * @param builder The {@link JsonObjectBuilder} to add the value to.
   * @param key The key for the attribute to be added.
   * @param value The value to be added providing it is <b>not</b>
   *              <code>null</code>.
   */
  protected void addString(JsonObjectBuilder builder, String key, String value)
  {
    if (value == null) return;
    builder.add(key, value);
  }

  /**
   * Conditionally adds the specified {@link Integer} to the specified
   * {@link JsonObjectBuilder} with the specified key if the specified
   * {@link Integer} value is <b>not</b> <code>null</code>.
   *
   * @param builder The {@link JsonObjectBuilder} to add the value to.
   * @param key The key for the attribute to be added.
   * @param value The value to be added providing it is <b>not</b>
   *              <code>null</code>.
   */
  protected void addInt(JsonObjectBuilder builder, String key, Integer value) {
    if (value == null) return;
    builder.add(key, value);
  }

  /**
   *
   */
  public void fromJson(JsonObject json) {

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
    put(map, CONCURRENCY,                  this.getConcurrency());
    put(map, MODULE_NAME,                  this.getModuleName());
    put(map, INIT_JSON,                    this.getJsonInitParameters());
    put(map, RABBIT_INFO_USER,             this.getRabbitInfoUser());
    put(map, RABBIT_INFO_PASSWORD,         this.getRabbitInfoPassword());
    put(map, RABBIT_INFO_HOST,             this.getRabbitInfoHost());
    put(map, RABBIT_INFO_PORT,             this.getRabbitInfoPort());
    put(map, RABBIT_INFO_VIRTUAL_HOST,     this.getRabbitInfoVirtualHost());
    put(map, RABBIT_INFO_QUEUE,            this.getRabbitInfoQueue());
    put(map, SQS_INFO_URL,                 this.getSqsInfoUrl());
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

  /**
   * The getters for the properties of this class.
   */
  private static final PropertyReflector PROPERTY_REFLECTOR
    = new PropertyReflector(SzReplicatorOptions.class);
}
