# Postgres Source Connector Documentation

## Overview

The Postgres Connector allows for efficient data ingestion from PostgreSQL databases using three main modes:

- **Snapshot**: Fetches all data from a specified table.
- **Snapshot with Cursor Column**: Fetches data in recurring intervals with a filter.
- **Realtime CDC (Change Data Capture)**: Captures ongoing changes to the database in real time.

This connector is controlled using JSON or YAML configuration based on the Go structure `PgSource`.

---

## Configuration

The connector can be configured using the `PgSource` structure. Below is the detailed explanation of each configuration option.

### JSON/YAML Example

```json
{
  "Hosts": ["host1", "host2"],
  "Database": "your-database",
  "User": "your-username",
  "Password": "your-password",
  "Port": 5432,
  "DBTables": ["table1", "table2"],
  "BatchSize": 1000,
  "SlotID": "replication_slot",
  "SlotByteLagLimit": 1000000,
  "TLSFile": "/path/to/tls/file",
  "KeeperSchema": "public",
  "CollapseInheritTables": true,
  "UsePolling": false,
  "ExcludedTables": ["table3"],
  "IsHomo": true,
  "NoHomo": false,
  "PreSteps": {},
  "PostSteps": {},
  "UseFakePrimaryKey": false,
  "IgnoreUserTypes": true,
  "IgnoreUnknownTables": true,
  "MaxBufferSize": 0,
  "ExcludeDescendants": false,
  "DesiredTableSize": 1000000,
  "SnapshotDegreeOfParallelism": 5,
  "EmitTimeTypes": false,
  "DBLogEnabled": false,
  "ChunkSize": 1000,
  "SnapshotSerializationFormat": "binary",
  "ShardingKeyFields": {
    "table1": ["id"],
    "table2": ["created_at"]
  },
  "PgDumpCommand": ["pg_dump", "-Fc", "-f", "dumpfile"]
}
```

### Fields

- **Host** (`string`, deprecated): Legacy field; prefer to use `Hosts`.

- **Hosts** (`[]string`): List of PostgreSQL hosts.

- **Database** (`string`): The name of the database from which data will be ingested.

- **User** (`string`): The database user with privileges to read from the specified tables.

- **Password** (`server.SecretString`): The password for the database user.

- **Port** (`int`): The port to connect to the database. Default: `5432`.

- **DBTables** (`[]string`): A list of tables to ingest from the database, support wildcard for table name, for example: `my_schema.*`, but not this: `my_schema.prefix_*`.

- **BatchSize** (`uint32`): The maximum number of rows to buffer internally when performing replication (not applicable for snapshots).

- **SlotID** (`string`): The replication slot to use for CDC.

- **SlotByteLagLimit** (`int64`): The byte lag limit for the replication slot.

- **TLSFile** (`string`): Content of TLS file for SSL connection

- **KeeperSchema** (`string`): Schema used for metadata storage.

- **CollapseInheritTables** (`bool`): Whether to collapse inherited tables into one logical table.

- **UsePolling** (`bool`): Whether to use polling instead of replication connection (CDC).

- **ExcludedTables** (`[]string`): A list of tables to exclude from ingestion.

- **IsHomo** (`bool`): Whether this is a homogeneous (same schema and table structure) setup.

- **NoHomo** (`bool`): Overrides `IsHomo`, forces heterogeneous relations.

- **AutoActivate** (`bool`): Whether the connector should auto-activate after setup.

- **PreSteps** (`PgDumpSteps`): Steps to run before the ingestion process begins, such as preparation of the environment, use for postgres to postgres migrations.

- **PostSteps** (`PgDumpSteps`): Steps to run after ingestion, such as cleanup operations, use for postgres to postgres migrations.

- **UseFakePrimaryKey** (`bool`): Use a fake primary key when the table does not have one.

- **IgnoreUserTypes** (`bool`): Ignore user-defined types.

- **IgnoreUnknownTables** (`bool`): If true, tables with unknown schemas are ignored.

- **MaxBufferSize** (`server.BytesSize`, deprecated): No longer in use.

- **ExcludeDescendants** (`bool`, deprecated): No longer in use; use `CollapseInheritTables` instead.

- **DesiredTableSize** (`uint64`): Desired partition size for snapshot sharding.

- **SnapshotDegreeOfParallelism** (`int`): Number of parallel snapshot parts.

- **DBLogEnabled** (`bool`): If true, use DBLog snapshot instead of the common snapshot.

- **ChunkSize** (`uint64`): Number of rows per chunk for DBLog snapshots. Automatically calculated if set to 0.

- **SnapshotSerializationFormat** (`PgSerializationFormat`): Format for snapshot serialization. Defaults to "binary" for homogeneous PostgreSQL transfers, and "text" for others.

- **ShardingKeyFields** (`map[string][]string`): Defines the sharding key fields for each table. Used for sharding data across multiple parts.

- **PgDumpCommand** (`[]string`): Command to run `pg_dump`, which can be used for snapshot-based ingestion.

---

## Supported transfer types

### 1. Snapshot

In the snapshot mode, the connector ingests all data from the specified tables in one go. The data is fetched based on the configuration fields `DBTables` and is not dependent on changes made to the database after the ingestion starts.

- **Use Case**: One-time ingestion of static data.
- **Performance Optimization**: Leverage `DesiredTableSize` and `SnapshotDegreeOfParallelism` to shard large tables across multiple processes.

### 2. Snapshot with Cursor Column

In this mode, the connector ingests data from the specified tables based on a filter column (like a timestamp or auto-incrementing ID). The ingestion occurs at regular intervals, copying only the new data based on the value of the cursor column.

- **Use Case**: Recurrent ingestion of new data with some form of time or ID-based filtering.

### 3. Realtime Change Data Capture (CDC)

The CDC mode listens for real-time changes (insertions, updates, deletions) in the database. It uses the PostgreSQL replication protocol with a replication slot to capture and ingest these changes.

- **Use Case**: Ongoing ingestion of live updates from the database.

---

## Advanced Configuration

### Sharding for Large Tables

To optimize ingestion performance, the connector allows you to shard large tables by specifying the desired shard size (`DesiredTableSize`) and the degree of parallelism (`SnapshotDegreeOfParallelism`). The ingestion process can be parallelized across multiple workers.

### Security

The connector supports secure connections via SSL by specifying the path to a TLS certificate (`TLSFile`) and configuring security groups (`SecurityGroupIDs`).

### Ignoring Specific Tables

If there are tables that should not be ingested, you can list them under `ExcludedTables` to exclude them from the ingestion process.

---

## Error Handling

1. **Unknown Table Schemas**: If a table's schema is unknown, the connector can be configured to either ignore it (`IgnoreUnknownTables`) or raise an error.
2. **No Primary Key**: The connector can generate a fake primary key for tables without one by enabling the `UseFakePrimaryKey` option.

---

## Demo

The Postgres  Connector offers flexible modes to ingest data either as a one-time snapshot, recurrent based on filters, or in real time via CDC. By configuring the various options available, the connector can be customized to meet different data ingestion requirements.

### Snapshot

![Made with VHS](https://vhs.charm.sh/vhs-3ETIytnxDtBmrgkcOX3ZBf.gif)

Here’s a step-by-step guide for the Postgres to Clickhouse transfer process, based on your `tape` format for `vhs`:

---

# Postgres to Clickhouse Transfer Guide

This guide demonstrates how to validate, check, and activate a data transfer from Postgres to Clickhouse using `trcli` and verifies the data in Clickhouse.

---

## 1. **Prepare Transfer Configuration**

Ensure that your transfer configuration file (`transfer.yaml`) is ready. This file contains all the settings for transferring data from Postgres to Clickhouse.

```yaml
type: SNAPSHOT_ONLY
src:
  type: pg
  params: |
    {
      "Hosts": ["localhost"],
      "User": "postgres",
      "Password": "password",
      "Database": "mydb",
      "Port": 5432
    }
dst:
  type: ch
  params: |
    {
      "ShardsList": [{"Hosts": ["localhost"]}],
      "HTTPPort": 8123,
      "NativePort": 9000,
      "Database": "default",
      "User": "default",
      "Password": "ch_password"
    }

```

## 2. **Validate the Transfer**

Use `trcli` to validate the configuration and ensure that everything is correct.

```bash
trcli validate --transfer transfer.yaml --log-config=minimal
```

- This will check the syntax and structure of the `transfer.yaml` file.

## 3. **Check Transfer Health**

Run a health check on the transfer configuration to confirm all necessary resources are available and ready for activation.

```bash
trcli check --transfer transfer.yaml --log-config=minimal
```

- This will validate connectivity, schema compatibility, and other checks before the transfer starts.

## 4. **Activate the Transfer**

Once the transfer configuration is validated and checked, activate the transfer to start the data migration process.

```bash
trcli activate --transfer transfer.yaml --log-config=minimal
```

- This command triggers the actual transfer of data from Postgres to Clickhouse.

## 5. **Verify Data in Clickhouse**

After the transfer is activated, connect to your Clickhouse instance to verify that the data has been transferred correctly.

```bash
clickhouse-client --host localhost --port 9000 --user default --password 'ch_password'
```

- Log into your Clickhouse database.

Now, run a query to check if the data is present:

```bash
select * from personas;
```

- This query checks the `personas` table for the transferred data.

---

### Tips:
- **Transfer Configuration**: Make sure your `transfer.yaml` file is correctly configured for both Postgres and Clickhouse.
- **Logs**: Adjust the `--log-config` option to view more detailed logs if necessary.

---

This process ensures a smooth and efficient data transfer from Postgres to Clickhouse using the `trcli` tool.
