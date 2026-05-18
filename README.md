# data-mart-replicator

If you are beginning your journey with
[Senzing](https://senzing.com/),
please start with
[Senzing Quick Start guides](https://docs.senzing.com/quickstart/).

## Overview

The Senzing Data Mart Replicator is a Java application that consumes
Senzing INFO messages from a message queue, retrieves entity data via
the Senzing Java SDK, and replicates statistics to a data mart database
(PostgreSQL or SQLite). The data mart provides a queryable relational
view of the resolved entities and their relationships, along with
pre-aggregated reports (Data Source Summary, Cross-Source Summary,
Entity Size Breakdown, and Entity Relation Breakdown).

## Setup and building

Before using the Data Mart Replicator you will need to build it.

### Dependencies

To build the Senzing Data Mart Replicator you will need Apache Maven
(recommend version 3.8 or later) as well as OpenJDK version 17 or later.

You will also need the Senzing product version 4.3.0 or later, which
provides the Senzing Java SDK and the native engine required at
runtime.

### Building

To build simply execute:

```console
mvn install
```

## Running

Running the Senzing Data Mart Replicator requires a database in which
to create the data mart tables that will be used to save the
statistics. While SQLite can be used for testing, it is limited to
single-connection writes and is not suitable for production. PostgreSQL
is recommended for production use and is required for multi-process
deployments. The command-line options let you configure the database
for the data mart.

The Senzing engine is initialized with the Senzing core settings JSON
(provided via `--core-settings` or the
`SENZING_ENGINE_CONFIGURATION_JSON` environment variable). While the
Data Mart Replicator itself does not write to the Senzing entity
repository, it does query it via the Senzing Java SDK to retrieve
current entity state.

The Senzing engine loading, modifying, or deleting records must
publish its INFO messages to one of the supported message queues:

- Amazon SQS
- RabbitMQ
- A SQL-based message queue table (`sz_message_queue`) in the data
  mart database itself

The INFO messages are consumed from one of these message queues, which
is configured via command-line options.

### Usage

To obtain command-line options, use the `--help` option:

```console
java -jar target/data-mart-replicator-server.jar --help
```

The output details all available command-line options and their
corresponding environment variables.

A typical invocation specifies:

- `--core-settings` — the Senzing engine configuration JSON (file
  path or inline JSON), or set the `SENZING_TOOLS_CORE_SETTINGS` or
  `SENZING_ENGINE_CONFIGURATION_JSON` environment variable.
- A message queue source: `--sqs-info-url`, `--rabbit-info-*`, or
  `--database-info-queue` (uses the data mart database).
- A data mart database: `--sqlite-database-file` or
  `--postgresql-*` options.

**Security note:** Passing credentials on the command line may expose
them to other users via process monitoring. Prefer the corresponding
environment variables (e.g., `SENZING_DATA_MART_POSTGRESQL_PASSWORD`)
for secrets.

Example using PostgreSQL for the data mart and SQS for the info
queue:

```console
java -jar target/data-mart-replicator-server.jar \
    --core-settings /etc/senzing/core-settings.json \
    --sqs-info-url https://sqs.us-west-2.amazonaws.com/.../my-queue \
    --postgresql-host db.example.com \
    --postgresql-port 5432 \
    --postgresql-database datamart \
    --postgresql-user datamart_user \
    --postgresql-password ${POSTGRES_PASSWORD}
```
