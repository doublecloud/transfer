# YTsaurus Destination Connector Documentation

## Overview

The YTsaurus Connector allows for efficient data insert from YTsaurus databases.
---

## Configuration

The YTsaurus Destination Connector is configured using the `YtDestination` structure. Below is a breakdown of each configuration field.

### JSON/YAML Example

#### Snapshot (Static Table)
```yaml
Path: "//home/dst_folder"
Cluster: "yt-backend:80"
Token: "token"
Static: true
```

#### Replication (Dynamic Table)
```yaml
Path: "//home/dst_folder"
Cluster: "yt-backend:80"
Token: "token"
CellBundle: "default",
PrimaryMedium: "default"
Static: false
```

### Fields

- **Path** (`string`): The path to the destination folder where the data will be written.
- **Cluster** (`string`): The address of the YTsaurus cluster. Default "hahn".
- **Token** (`string`): The token for the YTsaurus cluster.
- **PushWal** (`bool`): Storing the raw data stream in a separate table(__wal).
- **NeedArchive** (`bool`): An archive table will be created for each transfer table.
- **CellBundle** (`string`): [The tablet cell bundle](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/concepts) to use for dynamic tables quota in the YTsaurus cluster.
- **TTL** (`int64`): After specified time-to-live in milliseconds, the data will be deleted.
- **OptimizeFor** (`string`): Data in YTsaurus tables can be stored both in row-based `OptimizeFor=scan`, and columnar `OptimizeFor=lookup`. Defaults `OptimizeFor=scan`.
- **CanAlter** (`bool`): Change the data schema in tables when the schema in the source changes. Not all schema changes can be applied.
- **TimeShardCount** (`int`): TODO
- **Index** (`[]string`): For each specified column, a separate table will be created, where the specified column will be the primary key.
- **HashColumn** (`string`): The hash column to use for the data in the YTsaurus cluster.
- **PrimaryMedium** (`string`): [The primary medium](https://ytsaurus.tech/docs/en/user-guide/storage/media#primary) to use for the data in the YTsaurus cluster. Default "ssd_blobs" ??
- **Pool** (`string`): The pool to use for running merge and sort operations for static tables. Default "transfer_manager"
- **Strict** (`bool`): DEPRECATED, UNUSED IN NEW DATA PLANE - use LoseDataOnError and Atomicity.
- **Atomicity** (`yt.Atomicity`): [Atomicity](https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/transactions#atomicity) for the dynamic tables being created
- **LoseDataOnError** (`bool`): If true, some errors on data insertion to YTsaurus will be skipped, and a warning will be written to the log.
- **DiscardBigValues** (`bool`): If data is too long, batch will be discarded
- **TabletCount** (`int`): DEPRECATED - remove in March.
- **Rotation** (`*dp_model.RotatorConfig`): Use for partitioning tables.
  - **KeepPartCount** (`int`): The number of tables to be used by the rotator. The rotator will delete tables when the specified number of tables is exceeded.
  - **PartType** (`RotatorPartType`): Granularity of partitioning: by hour `h`, by day `d`, by month `m`. 
  - **PartSize** (`int`): Each table, created by the rotator, will contain a given number of partitions of the selected type.
  - **TimeColumn** (`string`): The column whose value will be used to split rows into time partitions. Leave blank to rotate by insertion time.
  - **TableNameTemplate** (`string`): Template for table name. Default template is "{{name}}/{{partition}}", where {{name}} is table name and {{partition}} is partition name based on timestamp.
- **VersionColumn** (`string`): TODO
- **AutoFlushPeriod** (`int`): Frequency of forced flushes [dynamic_store_auto_flush_period](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/compaction#flush_attributes), when the dynamic store is flushed to the disk straight away, even if it hasn't reached its overflow threshold yet.
- **Ordered** (`bool`): Will table be ordered?
- **TransformerConfig** (`map[string]string`): TOD
- **UseStaticTableOnSnapshot** (`bool`): Copy operations will be done with temporary static tables. For Drop cleanup policy existing data will be removed after finishing coping. With no cleanup policy merge of new and existing data will be done.
- **AltNames** (`map[string]string`): Rename tables
- **Cleanup** (`dp_model.CleanupType`): Cleanup policy for activate, reactivate and reupload processes: "Drop", "Truncate", "Disabled". Default "Drop".
- **Spec** (`YTSpec`): Overrides table settings. The file must contain a JSON object. Its properties will be included in the specification of each table created by the transfer.
- **TolerateKeyChanges** (`bool`): TODO
- **InitialTabletCount** (`uint32`): TODO
- **WriteTimeoutSec** (`uint32`): Timeout for write operations in seconds. Default 60 seconds.
- **ChunkSize** (`uint32`): ChunkSize defines the number of items in a single request to YTsaurus for dynamic sink and chunk size in bytes for static sink. Default 90_000            // items ??
- **BufferTriggingSize** (`uint64`): Bufferer trigging size . Default value (256 * humanize.MiByte) assume that we have 4 thread writer in 3gb box (default runtime box) so each thread would consume at most 256 * 2 (one time for source one time for target) mb + some constant memory in total it would eat 512 * 4 = 2gb, which is less than 3gb
- **BufferTriggingInterval** (`time.Duration`): Buffer trigging interval.
- **CompressionCodec** (`yt.ClientCompressionCodec`): [Compression codec](https://ytsaurus.tech/docs/en/user-guide/storage/compression#compression_codecs) for data.
- **DisableDatetimeHack** (`bool`): This disable old hack for inverting time. Time columns as int64 timestamp for LF>YTsaurus. ??
- **Connection** (`ConnectionData`): TODO
- **CustomAttributes** (`map[string]string`): Custom attributes for tables created in YSON format.
- **Static** (`bool`): Is table static?
- **SortedStatic** (`bool`): true, if we need to sort static tables.
- **StaticChunkSize** (`int`): desired size of static table chunk in bytes. Default 100 * 1024 * 1024 bytes

---

## Supported transfer types

### 1. Snapshot

In the snapshot mode, the connector ingests all data from the specified tables in one go. Better to use static tables in YTsaurus.

- **Use Case**: One-time ingestion of static data.
- **Performance Optimization**: Leverage `DesiredTableSize` and `SnapshotDegreeOfParallelism` to shard large tables across multiple processes.

### 2. Snapshot with Cursor Column

In this mode, the connector ingests data from the specified tables based on a filter column (like a timestamp or auto-incrementing ID). The ingestion occurs at regular intervals, copying only the new data based on the value of the cursor column. Need to use dynamic tables in YTsaurus.

- **Use Case**: Recurrent ingestion of new data with some form of time or ID-based filtering.

### 3. Replication

The Replication mode listens for real-time changes. Need to use dynamic tables in YTsaurus.

- **Use Case**: Ongoing ingestion of live updates from the database.

---


## Special Considerations

TODO
---

## Demo

TODO

