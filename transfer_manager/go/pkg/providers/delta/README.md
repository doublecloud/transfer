## Delta Provider

The Delta Provider is a Snapshot Provider for S3-compatible storages that handle Delta Lake data (see https://delta.io/ for details).

The implementation of the Delta read protocol is based on two canonical implementations:

1. Java standalone binary - https://docs.delta.io/latest/delta-standalone.html.
2. Rust implementation - https://github.com/delta-io/delta-rs

The standalone binary contains "Golden" Delta Lake datasets with all combinations of data, which can be found [here](https://github.com/delta-io/connectors/tree/master/golden-tables/src/test/resources/golden). To verify that everything works correctly, tests have been written using this "Golden" dataset stored in a sandbox.

Apart from the main provider and storage code, the implementation is divided into several sub-packages:

### Types

Contains type definitions for Delta Lake tables, which are inherited from Parquet. See the full list here: https://docs.databricks.com/sql/language-manual/sql-ref-datatypes.html

### Actions

Stores models of known Delta protocol log messages. Each message is stored as a one-of JSON row.

### Store

Provides an abstraction layer for the actual log directory storage. This interface is designed to be similar to its Java counterpart, which can be found here: https://github.com/delta-io/delta/blob/master/storage/src/main/java/io/delta/storage/LogStore.java. It is implemented with two storage options: local file system and S3-compatible storage.

### Protocol

Contains the main abstractions to work with Delta Lake:

- Table: Represents an actual table with a schema and multiple versions.
- Snapshot: Represents a specific version of a table, built from a list of actual Parquet files.
- TableLog: Represents all the table events, which can be used to calculate a snapshot for a specific version or timestamp.
- LogSegment: Represents a single event related to data.

### Workflow

The workflow for reading a Delta folder is as follows:

1. Check if the folder is a Delta folder (i.e., it has a `_delta_log` subdirectory).
2. List the contents of the `_delta_log` directory, with each file representing one version.
3. Read each file line by line in the `_delta_log` directory. Each line represents an Action event.
4. Replay the Action events to collect the remaining files in the table. Some events may add or remove files.
5. Read all the files that make up the table, as each file is an actual Parquet file with data related to the table.
