# data-mart-replicator

If you are beginning your journey with
[Senzing](https://senzing.com/),
please start with
[Senzing Quick Start guides](https://docs.senzing.com/quickstart/).

You are in the
[Senzing Garage](https://github.com/senzing-garage)
where projects are "tinkered" on.
Although this GitHub repository may help you understand an approach to using Senzing,
it's not considered to be "production ready" and is not considered to be part of the Senzing product.
Heck, it may not even be appropriate for your application of Senzing!

## Overview

The Senzing Data Mart Replicator is an application that leverages the Senzing Listener Java framework to consume Senzing INFO messages
from a message queue, obtain entity data from the Senzing G2 Java SDK, and update a set of data mart tables to track statistics on the
entity repository and the entities in it.

## Setup and building

Before using the Data Mart Replicator you will need to build it.

### Dependencies

To build the Senzing Listener you will need Apache Maven (recommend version 3.6.1 or later)
as well as OpenJDK version 11.0.x (recommend version 11.0.6+10 or later).

You will also need the Senzing product version 3.0.0 or later.

### Building

To build simply execute:

```console
mvn install
```

## Running

Running the Senzing Data Mart Replicator requires a database in which to create the data mart tables that will be used
to save the statistics. While SQLite can be used, it is limited to single connection access for writes. As such,
PostgreSQL is recommended. The command line options let you configure the database for the data mart. Additionally,
the Senzing G2 API is required along with a Senzing Entity Repository. While the Data Mart itself does not connect to
the Senzing Entity Repository, it initializes the Senzing G2 API which does connection using the Senzing Engine
Configuration JSON. Finally, the Senzing Engine that is loading, modifying or deleting records from the entity
repository will need to publish its INFO messages to an Amazon SQS message queue, Rabbit MQ message queue, or a Senzing
SQL database message queue provided through the Senzing Listener framework. The INFO messages can be consumed from
any of these message queues which is configured via the command-line options to the Data Mart Replicator.

### Usage:

To obtain command-line options execute use the `--help` option:

```console

java -jar data-mart-replicator-1.0.jar --help

```

The output will detail all the command-line options and environment variables to configure the Data Mart Replicator.

```console

java -jar data-mart-replicator-1.0.0.jar <options>

<options> includes:

[ Standard Options ]
   --help
        Should be the first and only option if provided.  Causes this help
        message to be displayed.
        NOTE: If this option is provided, the replicator will not start.

   --version
        Should be the first and only option if provided.  Causes the version
        of the G2 REST API Server to be displayed.
        NOTE: If this option is provided, the replicator will not start.

   --concurrency <thread-count>
        Sets the number of threads available for performing Senzing API
        operations (i.e.: the number of engine threads).  The number of
        threads for consuming messages and handling tasks is scaled based
        on the engine concurrency.  If not specified, then this defaults to 8.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_CONCURRENCY

   --module-name <module-name>
        The module name to initialize with.  If not specified, then the module
        name defaults to "senzing-datamart-replicator".
        --> VIA ENVIRONMENT: SENZING_DATA_MART_MODULE_NAME

   --verbose [true|false]
        Also -verbose.  If specified then initialize in verbose mode.  The
        true/false parameter is optional, if not specified then true is assumed.
        If specified as false then it is the same as omitting the option with
        the exception that omission falls back to the environment variable
        setting whereas an explicit false overrides any environment variable.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_VERBOSE

   --ini-file <ini-file-path>
        The path to the Senzing INI file to with which to initialize.
        EXAMPLE: -iniFile /etc/opt/senzing/G2Module.ini
        --> VIA ENVIRONMENT: SENZING_ENGINE_CONFIGURATION_INI_FILE

   --init-file <json-init-file>
        The path to the file containing the JSON text to use for Senzing
        initialization.  EXAMPLE: -initFile ~/senzing/g2-init.json
        --> VIA ENVIRONMENT: SENZING_ENGINE_CONFIGURATION_JSON_FILE

   --init-json <json-init-text>
        The JSON text to use for Senzing initialization.
        *** SECURITY WARNING: If the JSON text contains a password
        then it may be visible to other users via process monitoring.
        EXAMPLE: -initJson "{"PIPELINE":{ ... }}"
        --> VIA ENVIRONMENT: SENZING_ENGINE_CONFIGURATION_JSON

[ Asynchronous Info Queue Options ]
   The following options pertain to configuring the message queue from which to
   receive the "info" messages.  Exactly one such queue must be configured.

   --database-info-queue [true|false]
        Configures the data mart replicator to leverage the configured database
        to obtain the INFO messages via the sz_message_queue table.  If using a
        SQLite database you should ensure messages are not being written to the
        queue by another process at the same time they are being consumed since
        SQLite does not support concurrent writes from multiple connections
        --> VIA ENVIRONMENT: SENZING_DATA_MART_DATABASE_INFO_QUEUE

   --sqs-info-url <url>
        Specifies an Amazon SQS queue URL as the info queue.
        --> VIA ENVIRONMENT: SENZING_SQS_INFO_QUEUE_URL

   --rabbit-info-host <hostname>
        Used to specify the hostname for connecting to RabbitMQ as part of
        specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_HOST
                             SENZING_RABBITMQ_HOST (fallback)

   --rabbit-info-port <port>
        Used to specify the port number for connecting to RabbitMQ as part of
        specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_PORT
                             SENZING_RABBITMQ_PORT (fallback)

   --rabbit-info-user <user name>
        Used to specify the user name for connecting to RabbitMQ as part of
        specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_USERNAME
                             SENZING_RABBITMQ_USERNAME (fallback)

   --rabbit-info-password <password>
        Used to specify the password for connecting to RabbitMQ as part of
        specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_PASSWORD
                             SENZING_RABBITMQ_PASSWORD (fallback)

   --rabbit-info-virtual-host <virtual host>
        Used to specify the virtual host for connecting to RabbitMQ as part of
        specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_VIRTUAL_HOST
                             SENZING_RABBITMQ_VIRTUAL_HOST (fallback)

   --rabbit-info-queue <queue name>
        Used to specify the name of the RabbitMQ queue from which to pull the
        info messages.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_QUEUE

[ Data Mart Database Connectivity Options ]
   The following options pertain to configuring the connection to the data-mart
   database.  Exactly one such database must be configured.

   --sqlite-database-file <url>
        Specifies an SQLite database file to open (or create) to use as the
        data-mart database.  NOTE: SQLite may be used for testing, but because
        only one connection may be made, it will not scale for production use.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_SQLITE_DATABASE_FILE

   --postgresql-host <hostname>
        Used to specify the hostname for connecting to PostgreSQL as the
        data-mart database.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_POSTGRESQL_HOST

   --postgresql-port <port>
        Used to specify the port number for connecting to PostgreSQL as the
        data-mart database.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_POSTGRESQL_PORT

   --postgresql-database <database>
        Used to specify the database name for connecting to PostgreSQL as the
        data-mart database.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_POSTGRESQL_DATABASE

   --postgresql-user <user name>
        Used to specify the user name for connecting to PostgreSQL as the
        data-mart database.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_POSTGRESQL_USER

   --postgresql-password <password>
        Used to specify the password for connecting to PostgreSQL as the
        data-mart database.
        --> VIA ENVIRONMENT: SENZING_DATA_MART_POSTGRESQL_PASSWORD
```
