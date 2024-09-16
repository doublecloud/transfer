# S3 Source Connector Documentation

## Overview

The **S3 Source Connector** aggregates data from files stored in an S3-compatible storage bucket into a single table. It supports various file formats such as CSV, JSONL, and Parquet, and allows schema definition for the output data. The connector provides two modes of file replication: **polling** for new files or using an event-driven approach with **SQS** (Simple Queue Service).

This document describes the configuration options and behavior of the S3 Source Connector. The connector is controlled via JSON or YAML configurations based on the `S3Source` Go structure.

---

## Configuration

The S3 Source Connector is configured using the `S3Source` structure. Below is a breakdown of each configuration field.

### Example Configuration

```yaml
Bucket: "my-data-bucket"
ConnectionConfig:
AccessKey: "your-access-key"
SecretKey: "your-secret-key"
Endpoint: "s3.amazonaws.com"
UseSSL: true
VerifySSL: true
Region: "us-west-2"
PathPrefix: "data/2023/"
TableName: "s3_data_table"
TableNamespace: "my_namespace"
HideSystemCols: false
ReadBatchSize: 1000
InflightLimit: 5000000
InputFormat: "CSV"
OutputSchema:
- ColumnName: "id"
  DataType: "string"
- ColumnName: "value"
  DataType: "integer"
PathPattern: "*.csv"
Concurrency: 5
Format:
CSVSetting:
  Delimiter: ","
  QuoteChar: "\""
  EscapeChar: "\\"
  Encoding: "UTF-8"
  DoubleQuote: true
  BlockSize: 1048576
EventSource:
SQS:
  QueueName: "my-sqs-queue"
  OwnerAccountID: "123456789012"
  ConnectionConfig:
    AccessKey: "your-access-key"
    SecretKey: "your-secret-key"
    Endpoint: "sqs.us-west-2.amazonaws.com"
    UseSSL: true
    VerifySSL: true
    Region: "us-west-2"
UnparsedPolicy: "fail"
```

### Fields Breakdown

#### **Bucket** (`string`)
- Specifies the S3 bucket name from which the files will be retrieved.
- Example: `"my-data-bucket"`

#### **ConnectionConfig** (`ConnectionConfig`)
- Contains the configuration for connecting to the S3 bucket. It includes credentials, endpoint, region, and SSL settings.

  **Fields:**
    - `AccessKey`: The access key for the S3 bucket.
    - `SecretKey`: The secret key for the S3 bucket.
    - `Endpoint`: The S3-compatible endpoint (e.g., `"s3.amazonaws.com"`).
    - `UseSSL`: If set to `true`, the connection uses SSL.
    - `VerifySSL`: If set to `true`, the SSL certificate is verified.
    - `Region`: The AWS region where the bucket is hosted (e.g., `"us-west-2"`).

#### **PathPrefix** (`string`)
- Specifies the prefix of the file paths to filter the files in the S3 bucket.
- Example: `"data/2023/"`

#### **TableName** (`string`)
- The name of the output table where aggregated data from the files will be stored.
- Example: `"s3_data_table"`

#### **TableNamespace** (`string`)
- Defines the namespace for the table in which the data will be stored.
- Example: `"my_namespace"`

#### **HideSystemCols** (`bool`)
- If set to `true`, system columns (`__file_name` and `__row_index`) are excluded from the output schema.
- Example: `false`

#### **ReadBatchSize** (`int`)
- Specifies the number of rows read in each batch during ingestion.
- Example: `1000`

#### **InflightLimit** (`int64`)
- Limits the number of bytes that can be processed in-flight during replication.
- Example: `5000000`

#### **InputFormat** (`server.ParsingFormat`)
- The format of the input files. Supported formats include `CSV`, `JSONL`, and `Parquet`.
- Example: `"CSV"`

#### **OutputSchema** (`[]abstract.ColSchema`)
- Defines the schema for the aggregated table. This includes column names and data types.
- Example:
  ```yaml
  OutputSchema:
    - ColumnName: "id"
      DataType: "string"
    - ColumnName: "value"
      DataType: "integer"
  ```

#### **AirbyteFormat** (`string`)
- Used for backward compatibility with Airbyte. Specifies the raw format for later parsing.

#### **PathPattern** (`string`)
- A pattern that filters files to ingest, matching based on the file name (e.g., `"*.csv"`).

#### **Concurrency** (`int64`)
- Defines the number of concurrent processes for reading files.
- Example: `5`

#### **Format** (`Format`)
- Specifies the settings for the file format (CSV, JSONL, Parquet, etc.).

  **CSVSetting Fields:**
    - `Delimiter`: The delimiter for CSV files (e.g., `","`).
    - `QuoteChar`: The character used to quote fields (e.g., `"\""`).
    - `EscapeChar`: The character used to escape fields (e.g., `"\""`).
    - `Encoding`: The encoding of the file (e.g., `"UTF-8"`).
    - `DoubleQuote`: Whether double quotes are used in CSV fields.
    - `BlockSize`: The block size for reading the file in bytes (e.g., `1048576`).

#### **EventSource** (`EventSource`)
- Defines how new files are detected for replication. The connector can either poll for new files or listen for events from **SQS** (Simple Queue Service).

  **SQS Fields:**
    - `QueueName`: The name of the SQS queue.
    - `OwnerAccountID`: The AWS account ID of the queue owner.
    - `ConnectionConfig`: Configuration for connecting to SQS (similar to `ConnectionConfig` for S3).

#### **UnparsedPolicy** (`UnparsedPolicy`)
- Specifies the policy to follow when encountering unparsed or malformed files. Options are:
    - `"fail"`: Stop processing and throw an error.
    - `"continue"`: Skip the unparsed file and continue.
    - `"retry"`: Retry processing the file.

---

## Ingestion Modes

### Snapshot Mode

In **Snapshot Mode**, the S3 Source Connector collects all files from the specified bucket path and aggregates them into a single table. It reads the files based on the `PathPattern` and formats them according to the `InputFormat`.

### Event-Driven Mode with SQS

In this mode, the connector listens for file creation events using **Amazon SQS**. When new files are added to the S3 bucket, an event is triggered via SQS, and the connector ingests these files in near real-time.

---

## Supported File Formats

The connector supports the following file formats:
- **CSV**: Customizable with delimiters, quote characters, and encoding options.
- **JSONL**: Supports newline-separated JSON records.
- **Parquet**: Columnar storage format.

For each file format, the connector provides settings that can be configured to match the file's structure.

---

## Schema Definition

The S3 Source Connector requires the user to define the schema for the output table. The schema is specified in the `OutputSchema` field, which includes column names and data types. The connector then maps the input data from the files into this schema during ingestion.

---

## Example

TODO
