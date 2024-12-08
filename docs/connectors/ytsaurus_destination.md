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
- **PushWal** (`bool`): TODO
- **NeedArchive** (`bool`): Whether to archive the data in the YTsaurus cluster. ??
- **CellBundle** (`string`): [The tablet cell bundle](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/concepts) to use for dynamic tables quota in the YTsaurus cluster.
- **TTL** (`int64`): The time-to-live in milliseconds for the data in the YTsaurus cluster.
- **OptimizeFor** (`string`): Data in YTsaurus tables can be stored both in row-based `OptimizeFor=scan`, and columnar `OptimizeFor=lookup`. Defaults `OptimizeFor=scan`.
- **CanAlter** (`bool`): Can be altered table schema?
- **TimeShardCount** (`int`): TODO
- **Index** (`[]string`): TODO
- **HashColumn** (`string`): The hash column to use for the data in the YTsaurus cluster.
- **PrimaryMedium** (`string`): [The primary medium](https://ytsaurus.tech/docs/en/user-guide/storage/media#primary) to use for the data in the YTsaurus cluster. Default "ssd_blobs" ??
- **Pool** (`string`): The pool to use for running merge and sort operations for static tables. Default "transfer_manager"
- **Strict** (`bool`): DEPRECATED, UNUSED IN NEW DATA PLANE - use LoseDataOnError and Atomicity.
- **Atomicity** (`yt.Atomicity`): [Atomicity](https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/transactions#atomicity) for the dynamic tables being created
- **LoseDataOnError** (`bool`): If true, some errors on data insertion to YTsaurus will be skipped, and a warning will be written to the log.
- **DiscardBigValues** (`bool`): If data is too long, batch will be discarded
- **TabletCount** (`int`): DEPRECATED - remove in March.
- **Rotation** (`*dp_model.RotatorConfig`): Use for partitioning tables.
  - **KeepPartCount** (`int`): Number of partitions to keep.
  - **PartType** (`RotatorPartType`): Type of partitioning: by hour `h`, by day `d`, by month `m`. 
  - **PartSize** (`int`): Max size of partition.
  - **TimeColumn** (`string`): Time column for partitioning.
  - **TableNameTemplate** (`string`): Template for table name.
- **VersionColumn** (`string`): TODO
- **AutoFlushPeriod** (`int`): Frequency of forced flushes [dynamic_store_auto_flush_period](https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/compaction#flush_attributes), when the dynamic store is flushed to the disk straight away, even if it hasn't reached its overflow threshold yet.
- **Ordered** (`bool`): Will table be ordered?
- **TransformerConfig** (`map[string]string`): TOD
- **UseStaticTableOnSnapshot** (`bool`): If true, use static tables on snapshot.
- **AltNames** (`map[string]string`): Rename tables
- **Cleanup** (`dp_model.CleanupType`): Policy for cleanup data: "Drop", "Truncate", "Disabled". Default "Drop".
- **Spec** (`YTSpec`): TODO
- **TolerateKeyChanges** (`bool`): TODO
- **InitialTabletCount** (`uint32`): TODO
- **WriteTimeoutSec** (`uint32`): Timeout for write operations in seconds. Default 60 seconds.
- **ChunkSize** (`uint32`): ChunkSize defines the number of items in a single request to YTsaurus for dynamic sink and chunk size in bytes for static sink. Default 90_000            // items ??
- **BufferTriggingSize** (`uint64`): Bufferer trigging size . Default value (256 * humanize.MiByte) assume that we have 4 thread writer in 3gb box (default runtime box) so each thread would consume at most 256 * 2 (one time for source one time for target) mb + some constant memory in total it would eat 512 * 4 = 2gb, which is less than 3gb
- **BufferTriggingInterval** (`time.Duration`): Buffer trigging interval.
- **CompressionCodec** (`yt.ClientCompressionCodec`): [Compression codec](https://ytsaurus.tech/docs/en/user-guide/storage/compression#compression_codecs) for data.
- **DisableDatetimeHack** (`bool`): This disable old hack for inverting time. Time columns as int64 timestamp for LF>YTsaurus. ??
- **Connection** (`ConnectionData`): TODO
- **CustomAttributes** (`map[string]string`): TODO
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

