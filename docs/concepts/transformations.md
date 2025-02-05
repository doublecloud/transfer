---
title: "Transformations in {{ data-transfer-name }}"
description: "Transform the data you replicate with Transfer on the fly with built-in transformers. Convert values, apply filters, perform SQL-like transformations, and more"
---

# Transformations in {{ data-transfer-name }}

{{ DC }} {{ data-transfer-name }} can apply transformations to the transferred data on the fly.

You can configure transformations when creating or editing a transfer.
To do that, click **+ Transformation** on the transfer configuration page and provide the desired settings.

The list of available transformations in each transfer depends on the source and target types.

## What is transformer

We can apply stateless transformation on our `inflight` data.
This is based on our data model

![data-model](../transformers/assets/data_model_transformer.png)

Each batch of changes can be transformed into new batch of changes:

![transformation](../transformers/assets/transformer_data_flow.png)

### How to add new transformer

1. Create new package
2. Implemenet `abstract.Transformer` interface
3. Register implementation

Example:

#### Implementation of `abstract.Transformer`

```go
type DummyTransformer struct {
}

func (r *DummyTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (r *DummyTransformer) Suitable(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

func (r *DummyTransformer) ResultSchema(original abstract.TableColumns) abstract.TableColumns {
	return original
}

func (r *DummyTransformer) Description() string {
	return "this transformer do nothing"
}
```

## SQL

Performs SQL-like in-memory data transformations based on a provided query in the {{ CH }} SQL dialect.
The transformer is based on
[{{ CH }} Local ![external link](../_assets/external-link.svg)](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/) —
an isolated {{ CH }} database engine.

The source table inside {{ CH }} Local is named `table`.
The {{ CH }} table structure mimics the source table structure.

Because each source change item (row) contains extra metadata,
source and target data must match.
Therefore, each row must have a key defined.
All these keys must be unique in every batch.
For that, a collapse function is called.

If Transfer can't match source keys with the transformed data, it marks such rows as containing an error.

{% cut "SQL query example" %}

When writing an SQL query, you must preserve original key-columns:

```sql
SELECT
   parseDateTime32BestEffortJSONExtractString(CloudTrailEvent, 'eventTime')) AS eventTime,
   JSONExtractString(CloudTrailEvent, 'http_request.user_agent') AS http_useragent,    
   JSONExtractString(CloudTrailEvent, 'errorMessage') AS error_message,
   JSONExtractString(CloudTrailEvent, 'errorCode') AS error_kind, 
   JSONExtractString(CloudTrailEvent, 'sourceIPAddress') AS network_client_ip,
   JSONExtractString(CloudTrailEvent, 'eventVersion') AS eventVersion,
   JSONExtractString(CloudTrailEvent, 'eventSource') AS eventSource,
   JSONExtractString(CloudTrailEvent, 'eventName') AS eventName,
   JSONExtractString(CloudTrailEvent, 'awsRegion') AS awsRegion,
   JSONExtractString(CloudTrailEvent, 'sourceIPAddress') AS sourceIPAddress,
   JSONExtractString(CloudTrailEvent, 'userAgent') AS userAgent,
   JSONExtractString(CloudTrailEvent, 'requestID') AS requestID,
   JSONExtractString(CloudTrailEvent, 'eventID') AS eventID,
   JSONExtractBool(CloudTrailEvent, 'readOnly') AS readOnly,
   JSONExtractString(CloudTrailEvent, 'eventType') AS eventType,
   JSONExtractBool(CloudTrailEvent, 'managementEvent') AS managementEvent,
   JSONExtractString(CloudTrailEvent, 'recipientAccountId') AS recipientAccountId,
   JSONExtractString(CloudTrailEvent, 'eventCategory') AS eventCategory,
   JSONExtractString(CloudTrailEvent, 'aws_account') AS account,

   JSONExtractString(CloudTrailEvent, 'userIdentity.type') AS userIdentity_type,
   JSONExtractString(CloudTrailEvent, 'userIdentity.principalId') AS userIdentity_principalId,
   JSONExtractString(CloudTrailEvent, 'userIdentity.arn') AS userIdentity_arn,
   JSONExtractString(CloudTrailEvent, 'userIdentity.accountId') AS userIdentity_accountId,
   JSONExtractString(CloudTrailEvent, 'userIdentity.accessKeyId') AS userIdentity_accessKeyId,

   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.type') AS sessionIssuer_type,
   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.principalId') AS sessionIssuer_principalId,
   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.arn') AS sessionIssuer_arn,
   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.accountId') AS sessionIssuer_accountId,
   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.userName') AS sessionIssuer_userName,

   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.webIdFederationData.federatedProvider') AS federatedProvider,

   JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.attributes.creationDate') AS attributes_creationDate,
   JSONExtractBool(CloudTrailEvent, 'userIdentity.sessionContext.attributes.mfaAuthenticated') AS attributes_mfaAuthenticated,

   JSONExtractString(CloudTrailEvent, 'requestParameters.commandId') AS requestParameters_commandId,
   JSONExtractString(CloudTrailEvent, 'requestParameters.instanceId') AS requestParameters_instanceId,

   JSONExtractString(CloudTrailEvent, 'tlsDetails.tlsVersion') AS tlsDetails_tlsVersion,
   JSONExtractString(CloudTrailEvent, 'tlsDetails.cipherSuite') AS tlsDetails_cipherSuite,
   JSONExtractString(CloudTrailEvent, 'tlsDetails.clientProvidedHostHeader') AS tlsDetails_clientProvidedHostHeader
FROM table
```

{% endcut %}

## Mask secret fields

Applies a hash function to specified columns in tables to further protect sensitive data during transfer.

## Columns filter

Applies filtering to the list of columns transferred from the data source.

## Rename tables

Associates a table name on the source with a new table name on the target 
without changing the contents of the transferred table.

## Replace primary key

Reassigns the primary key column on the target table.

## Convert values to string

Converts a certain data column in a specified table to a string.

## Convert data to raw JSON

Converts a certain data column in a specified table to raw JSON.

## Convert CDC data to raw JSON with history

Converts the 
[change data capture (CDC) data ![external link](../_assets/external-link.svg)](https://en.wikipedia.org/wiki/Change_data_capture)
to raw JSON with history.

# Apply Transformations

For example, you don't want to transfer the column with passwords.
In **Transformer list**, click **Add transformer** to create a set of transformation rules.
Add this column's name to the `Exclude columns` section.
In **Transformer[number]** → **Columns filter**, configure transformations for tables and columns:

* **Include columns** sets the list of columns to transfer and **Exclude columns** makes the list of columns that won't be transferred. Set these table names as regular expressions.

Each transformer is a separate set of rules, and you can combine different rules within each set.

For example, you can set a table in `Include tables` and an `Exclude column`. In this case, the service will ignore the specified `Exclude columns` only for the included table. If you combine `Exclude tables` and `Include columns`, only the specified columns will be transferred from all tables except those specified in the`Exclude tables` field.

## Managing transformers

Select the appropriate transformation type:

{% cut "Mask secret fields" %}

This transformation allows you to apply a hash function to specified columns in tables to further protect sensitive data during transfer.

1. Under **Tables**, specify the following:

    * **Included tables** restricts the set of tables to transfer.

    * **Excluded tables** allow transferring all data except the specified tables.

   Set these table names as regular expressions:

   {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

1. Under **Column list**, click **+** to add a column name. The masking will be applied to the columns listed in this section.

1. Under **Mask function** → **Hash** → **User-defined Salt**, specify the [Salt hash ![external link](../_assets/external-link.svg)](https://doubleoctopus.com/security-wiki/encryption-and-cryptography/salted-secure-hash-algorithm/) you want to apply to your data.

{% endcut %}

{% cut "Columns filter" %}

This transformation allows you to apply filtering to the list of columns to transfer from the data source.

1. Under **Tables**, click **+ Tables** and specify the following:

    * **Included tables** restricts the set of tables to transfer.

    * **Excluded tables** allow transferring all data except the specified tables.

   Set these table names as regular expressions:

   {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

1. Under **Columns**, specify the following:

    * **Included columns** restricts the set of columns to transfer from the tables specified above.

    * **Excluded columns** allow transferring all columns except the specified ones.

   Set these column names as regular expressions:

   {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

{% endcut %}

{% cut "Rename tables" %}

This transformation gives you a capability to associate the table name on the source with a new table name on the target without changing the contents of the transferred table.

```yaml
  transformation:
    transformers:
      - renameTables:
          renameTables:
            - newName:
                name: old_name
                nameSpace: 'public'
              originalName:
                name: new_name
                nameSpace: fancy_shmancy
```

{% cut "Replace primary key" %}

This transformation allows you to reassign the primary key column on the target table.

1. Under **Tables**, click **+ Tables** and specify the following:

    * **Included tables** restricts the set of tables to transfer.

    * **Excluded tables** allow transferring all data except the specified tables.

   Set these table names as regular expressions:

   {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

1. Under **Key columns names**, specify the pairs of columns to replace separated by a `,` comma as follows:

   ```sh
   <column name at the source> <column name at the target>, 
   ```

   {% note warning %}

   Assigning two or more primary keys per table makes these tables incompatible with {{ CH }}.

   {% endnote %}

{% endcut %}

{% cut "Convert values to string" %}

This transformation allows you to convert a certain data column in a specified table to a string.

```yaml
transformation:
   transformers:
      - raw_doc_grouper:
         tables:
              IncludeRegexp: ''
              ExcludeRegexp: ''
         columns: []
```

The conversion will be applied to the columns listed in both sections.

{% endcut %}

{% cut "Convert data to raw JSON" %}

This transformation gives you a capability to convert a certain data column in a specified table to a raw JSON.

```yaml
transformation:
   transformers:
      - raw_doc_grouper:
         tables: []
         keys: []
         fields: []
```

{% endcut %}

{% cut "Sharding" %}

This transformation allows you to distribute the tables between multiple shards on the {{ CH }} data destination.

```yaml
transformation:
   transformers:
      - sharder_transformer:
         tables: []
         columns: []
         shardsCount: '42'
```

{% endcut %}

{% cut "Convert CDC data to raw JSON with history" %}

This transformation allows you to convert the [change data capture (CDC) data ![external link](../_assets/external-link.svg)](https://en.wikipedia.org/wiki/Change_data_capture) to raw JSON with history.

```yaml
transformation:
   transformers:
      - raw_cdc_doc_grouper:
         tables: []
         keys: []
         fields: []
```

{% endcut %}

{% cut "SQL" %}

Here is basic example of SQL transformer:

```yaml
transformation:
   transformers:
      - sql:
            tables:                                                                       
                include_tables:                                                             
                - '"public"."included_data"'                                                
                exclude_tables:                                                             
                - '"public"."excluded_data"'                                                
            query: |
                select
                    *, 42 as important_column
                from table
```

{% endcut %}

{% cut "dbt" %}

Apply your dbt project to the snapshot of the data transferred to {{ CH }}.

For more information on how to apply dbt transformation to your data.

1. Specify the address of the **GitRepositoryLink** containing your dbt project. It must start with `https://`. The root directory of the repository must contain a `dbt_project.yml` file.
1. Under **GitBranch**, specify the branch or a tag of the git repository containing your dbt project.
1. Provide the **ProfileName** which will be created automatically using the settings of the destination endpoint. The name must match the `profile` property in the `dbt_project.yml` file.
1. From the dropdown list, select the **Operation** for your dbt project to perform. For more information, see the [official dbt documentation ![external link](../_assets/external-link.svg)](https://docs.getdbt.com/docs/build/hooks-operations#about-operations).

```yaml
transformation:
   transformers:
      - dbt:                                               
         ProfileName: ''
         GitBranch: ''
         GitRepositoryLink: ''
         Operation: ''
```

{% endcut %}


# Configuration Model
# DoubleCloud Transfer YAML Transformations

## 1. SQL Transformer
- **Purpose**: Performs SQL-like in-memory data transformations based on a provided query in the ClickHouse® SQL dialect.
- **Configuration**:
   - `query`: The SQL query to execute.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - sql:
      query: SELECT * FROM table
      tables:
        includeTables:
          - public.test
        excludeTables: null
    transformerId: ""
  ```


This transformer accepts {{ CH }} SQL dialect and allows you to produce SQL-like in-memory data transformation. The solution is based on [{{ CH }} Local ![external link](../_assets/external-link.svg)](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/)

The source table inside {{ CH }} Local is named `table`, the {{ CH }} table structure mimics the source table structure.

Since each source change item (row) contains extra metadata, we must match source and target data. Therefore, each row must have a key defined. All these keys should be unique in every batch. Do do this, we call a collapse function.

If we can't match source keys with transformed data, we mark such row as containing an error.

When writing an SQL query, you must preserve original key-columns:

```sql
SELECT
  parseDateTime32BestEffortJSONExtractString(CloudTrailEvent, 'eventTime')) AS eventTime,
  JSONExtractString(CloudTrailEvent, 'http_request.user_agent') AS http_useragent,    
  JSONExtractString(CloudTrailEvent, 'errorMessage') AS error_message,
  JSONExtractString(CloudTrailEvent, 'errorCode') AS error_kind, 
  JSONExtractString(CloudTrailEvent, 'sourceIPAddress') AS network_client_ip,
  JSONExtractString(CloudTrailEvent, 'eventVersion') AS eventVersion,
  JSONExtractString(CloudTrailEvent, 'eventSource') AS eventSource,
  JSONExtractString(CloudTrailEvent, 'eventName') AS eventName,
  JSONExtractString(CloudTrailEvent, 'awsRegion') AS awsRegion,
  JSONExtractString(CloudTrailEvent, 'sourceIPAddress') AS sourceIPAddress,
  JSONExtractString(CloudTrailEvent, 'userAgent') AS userAgent,
  JSONExtractString(CloudTrailEvent, 'requestID') AS requestID,
  JSONExtractString(CloudTrailEvent, 'eventID') AS eventID,
  JSONExtractBool(CloudTrailEvent, 'readOnly') AS readOnly,
  JSONExtractString(CloudTrailEvent, 'eventType') AS eventType,
  JSONExtractBool(CloudTrailEvent, 'managementEvent') AS managementEvent,
  JSONExtractString(CloudTrailEvent, 'recipientAccountId') AS recipientAccountId,
  JSONExtractString(CloudTrailEvent, 'eventCategory') AS eventCategory,
  JSONExtractString(CloudTrailEvent, 'aws_account') AS account,

  JSONExtractString(CloudTrailEvent, 'userIdentity.type') AS userIdentity_type,
  JSONExtractString(CloudTrailEvent, 'userIdentity.principalId') AS userIdentity_principalId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.arn') AS userIdentity_arn,
  JSONExtractString(CloudTrailEvent, 'userIdentity.accountId') AS userIdentity_accountId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.accessKeyId') AS userIdentity_accessKeyId,
  
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.type') AS sessionIssuer_type,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.principalId') AS sessionIssuer_principalId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.arn') AS sessionIssuer_arn,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.accountId') AS sessionIssuer_accountId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.userName') AS sessionIssuer_userName,
  
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.webIdFederationData.federatedProvider') AS federatedProvider,
  
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.attributes.creationDate') AS attributes_creationDate,
  JSONExtractBool(CloudTrailEvent, 'userIdentity.sessionContext.attributes.mfaAuthenticated') AS attributes_mfaAuthenticated,
  
  JSONExtractString(CloudTrailEvent, 'requestParameters.commandId') AS requestParameters_commandId,
  JSONExtractString(CloudTrailEvent, 'requestParameters.instanceId') AS requestParameters_instanceId,
  
  JSONExtractString(CloudTrailEvent, 'tlsDetails.tlsVersion') AS tlsDetails_tlsVersion,
  JSONExtractString(CloudTrailEvent, 'tlsDetails.cipherSuite') AS tlsDetails_cipherSuite,
  JSONExtractString(CloudTrailEvent, 'tlsDetails.clientProvidedHostHeader') AS tlsDetails_clientProvidedHostHeader
FROM table
```


## 2. Mask Field Transformer
- **Purpose**: Applies a hash function to specified columns to protect sensitive data during transfer.
- **Configuration**:
   - `columns`: List of columns to mask.
   - `maskFunctionHash`: Defines the hash function and includes a user-defined salt for hashing.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - maskField:
      columns:
        - address
      maskFunctionHash:
        userDefinedSalt: random_secret_string
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```

## 3. Filter Columns Transformer
- **Purpose**: Filters the list of columns transferred from the data source.
- **Configuration**:
   - `columns`:
      - `includeColumns`: List of columns to include (supports regular expressions).
      - `excludeColumns`: List of columns to exclude.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - filterColumns:
      columns:
        includeColumns:
          - ^.*
        excludeColumns: null
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```

## 4. Rename Tables Transformer
- **Purpose**: Renames tables during the transfer process.
- **Configuration**:
   - `renameTables`: List of renaming rules, each specifying the original and new table names and namespaces.
- **Example**:
  ```yaml
  - renameTables:
      renameTables:
        - originalName:
            name: foo
            nameSpace: public
          newName:
            name: schmancy
            nameSpace: fancy
    transformerId: ""
  ```

## 5. Replace Primary Key Transformer
- **Purpose**: Reassigns the primary key columns on the target table.
- **Configuration**:
   - `keys`: List of columns to be used as primary keys.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - replacePrimaryKey:
      keys:
        - id
        - first_name
        - last_name
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```

## 6. Convert to String Transformer
- **Purpose**: Converts specified data columns in a table to string format.
- **Configuration**:
   - `columns`:
      - `includeColumns`: List of columns to include for conversion.
      - `excludeColumns`: List of columns to exclude from conversion.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - convertToString:
      columns:
        includeColumns:
          - salary
        excludeColumns: null
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```

## 7. Raw Document Grouper Transformer
- **Purpose**: Converts data into raw JSON format, grouping specified fields.
- **Configuration**:
   - `keys`: List of primary key columns for grouping.
   - `fields`: List of fields to be grouped into a raw document.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - rawDocGrouper:
      keys:
        - id
      fields:
        - salary
        - address
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```

## 8. Raw CDC Document Grouper Transformer
- **Purpose**: Similar to the Raw Document Grouper but optimized for Change Data Capture (CDC) processing, grouping fields based on a specified key.
- **Configuration**:
   - `keys`: List of primary key columns for grouping.
   - `fields`: List of fields to be grouped into a raw document.
   - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - rawCdcDocGrouper:
      keys:
        - id
      fields:
        - salary
        - address
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```

## 9. Lambda Transformer
- **Purpose**: Calls an external cloud function to process data before storing it in the target database.
- **Configuration**:
   - `Options`:
      - `CloudFunction`: The name of the external cloud function.
      - `CloudFunctionsBaseURL`: The base URL for the function service (e.g., AWS Lambda, Google Cloud Functions).
      - `InvocationTimeout`: Maximum execution time for the function.
      - `NumberOfRetries`: Number of retries in case of failure.
      - `BufferSize`: Size of the buffer before sending data.
      - `BufferFlushInterval`: Interval for flushing the buffer.
      - `Headers`: Any additional headers required for API calls.
   - `TableID`: Defines the target table for transformation.
- **Example**:
  ```yaml
  - lambda:
      Options:
        CloudFunction: test_func
        CloudFunctionsBaseURL: aws_url
        InvocationTimeout: 1e+10
        NumberOfRetries: 3
        BufferSize: 1.048576e+06
        BufferFlushInterval: 1e+09
        Headers: null
      TableID:
        Name: test
        Namespace: public
    transformerId: ""
  ```

## 10. DBT Transformer
- **Purpose**: Executes a dbt model to transform data within the target database.
- **Configuration**:
   - `ProfileName`: Name of the dbt profile matching the `profile` property in `dbt_project.yml`.
   - `GitBranch`: Branch or tag of the Git repository containing the dbt project.
   - `GitRepositoryLink`: URL to the Git repository with the dbt project (must start with `https://`).
   - `Operation`: The operation to execute within the dbt project.
- **Example**:
  ```yaml
  - dbt:
      ProfileName: ''
      GitBranch: ''
      GitRepositoryLink: ''
      Operation: ''
  ```

---

## Additional Configuration Options
- `debugmode`: Enables or disables debug mode for transformation execution.
- `errorsoutput`: Defines how transformation errors should be handled.
- `data_objects.include_objects`: Specifies which data objects should be included in the transformation process.
- `type_system_version`: Defines the transformation system version to ensure compatibility.

This YAML-based approach to configuring transformations in **DoubleCloud Transfer** provides a structured and flexible way to modify and optimize data flows, ensuring smooth and efficient data pipeline operations.
