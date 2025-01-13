---
title: "{{ PG }} connector"
description: "Configure the {{ PG }} connector to transfer data to and from {{ PG }} databases with {{ DC }} {{ data-transfer-name }}"
---

# {{ PG }} connector

You can use the {{ PG }} connector in both **source** and **target** endpoints.
In source endpoints, the connector retrieves data from {{ PG }} databases.
In target endpoints, it inserts data to {{ PG }} databases.

## Source endpoint

{% list tabs %}

* Configuration

    {% note warning "Requirements for source {{ PG }} databases" %}

    1. The transferred data must be in tables, not views.

    1. Each table must have a primary key.

    {% endnote %}

    To configure a source {{ PG }} endpoint, provide the following settings:

    1. Configure the connection: 

        1. Under **Connection settings** → **Connection type**,
            expand **Hosts**.
           
        1. Click ![plus icon](../_assets/plus-sign.svg) 
            and enter the host's IP address or domain name (FQDN).
            To add several hosts, repeat this step.

        1. In **Port**, enter the port for connection, or leave the default value of `5432`.

        1. To encrypt data transmission, upload a `.pem` file with certificates under **CA Certificate**.

        1. In **Database**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

            {% note warning %}

            The user must have a role with the `REPLICATION` attribute or be a superuser.
            [Learn more ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/logical-replication-security.html)

            {% endnote %}

        {% cut "Standard {{ PG }} connection string reference" %}

        Typically, a {{ PG }} connection string looks as follows:
       
        ```shell
        postgres://<username>:<password>@<hostname>:<port>/<database>
        ```

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<port>`: Port. Default is `5432`.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

   1. Configure the **Table filter** if you need to transfer only specific tables. If you don't specify any of the settings below, all tables will be transferred.

      * **Included tables**

          {{ data-transfer-name }} will transfer only the data from these tables.
          Specify the table names together with the schema in the `schema_name.target_table` format.

         Enter `schema_name.*` to select all tables.

      * **Excluded tables**

         The data from the tables on this list won't be transferred. Specify the table names after the name of the schema containing these tables as follows: `schema_name.excluded_table`.

         Enter `schema_name.*` to select all tables.

   1. Configure **Schema migration**:

      {% note tip %}

      The default schema migration settings usually let you successfully perform a transfer. Change the settings of the initial and final stages of the transfer only if necessary.

      {% endnote %}

      During the transfer process, the database schema is transferred from the source to the destination. The service uses [pg_dump ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/app-pgdump.html) to transfer the schema. You can set the migration rules for each object class separately. The possible options are *At begin*, *At end*, *Don't copy*.

      The transfer is performed in two stages:

      * **Activation stage**

         This stage is performed on activation of the transfer, before [snapshotting](../concepts/transfer-types.md#snapshot) or replication to create schema on the target.

         You can select parts of the schema to be transferred at this stage.

         By default, they include the following:

         * **Sequences**
         * **Owned sequences**
         * **Current values for sequences**
         * **Tables**
         * **Primary keys**
         * **Default values**
         * **Views**
         * **Functions**
         * **Types**
         * **Rules**
         * **Collations**
         * **Policies**

      * **Deactivation stage**

         This stage is performed at the end of the transfer upon its deactivation.

         If the transfer is constantly working in replication mode ([Replication](../concepts/transfer-types.md#replication) and [Snapshot and replication](../concepts/transfer-types.md#snapshot-and-replication) transfer types), then the final stage of the transfer will be performed only after the replication stops. You can choose which parts of the schema to migrate.

         At this stage, it's assumed that when the transfer is deactivated, there is no writing activity on the source. For complete reliability, the source is set to read-only mode. The database schema on the target is brought to a state where it will be consistent with the schema on the source.

         We recommend that you include resource-intensive operations, such as index transfers, in the final migration stage. Moving the indexes at the beginning of the transfer can slow it down.

         Default objects migrated at the end of a transfer are the following:

         * **Foreign keys**
         * **Constraints**
         * **Indexes**
         * **Triggers**

      {% note info %}

      When the transfer is restarted during the replication phase, the table schemas on the target are preserved. In this case, the service will transfer only the table schemas that aren't in the target at the restart.

      {% endnote %}

   1. Specify **Advanced** settings:

      * **Maximum WAL size for the replication slot**. Here you can set the maximum size of [Write-Ahead Log ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/wal-intro.html) keeping on the replication slot. When this value is exceeded, replication stops and the replication slot is removed.

      * The name of the **Database schema for auxiliary tables**.

      * Check the **Merge inherited tables** box if you need to merge content of the tables.

      * Under **Parallel table copying settings**:

         * **Split threshold in bytes**

            This setting defines the minimum tables size in bytes starting from which the service will parallelize the tables. To the right from the input field, select the increment from the dropdown list - from `B` (bytes) up to `GB` (gigabytes).

         * **Maximum number of table parts**

            The maximum number of parts into which the service will split a table when using sharded loading.

   {% note tip "Partition replication for partitioned tables" %}

   If you need to replicate all the partitions of a partitioned table, do the following:

   * Add the table in question to the **Table filter** → **Included tables list**.

   * Check the **Advanced settings** → **Merge inherited tables** box.

   This will result in the source endpoint extracting the partitioned table without partitions - the extracted items' table name will be set to the name of the partitioned table, even if the item represents a row of a partition.

   {% endnote %}

* Source data type mapping

    | **{{ PG }} type** | **{{ data-transfer-name }} type** |
    |---|---|
    | `BIGINT` | int64 |
    | `INTEGER` | int32 |
    | `SMALLINT` | int16 |
    | — | int8 |
    | — | uint64 |
    | — | uint32 |
    | — | uint16 |
    | — | uint8 |
    | — | float |
    | `NUMERIC`, `REAL`, `DOUBLE PRECISION` | double |
    | `BIT (N)`, `BIT VARYING (N)`, `BYTEA`, `BIT`, `BIT VARYING` | string |
    | `CHARACTER VARYING`, `DATA`, `UUID`, `NAME`, `TEXT`, `INTERVAL`, `TIME WITH TIME ZONE`, `TIME WITHOUT TIME ZONE`, `CHAR`, `ABSTIME`, `MONEY` | utf8 |
    | `BOOLEAN` | boolean |
    | `DATE` | date |
    | — | datetime |
    | `TIMESTAMP WITHOUT TIME ZONE`, `TIMESTAMP WITH TIME ZONE` | timestamp |
    | `ARRAY`, `CHARACTER`, `CITEXT`, `HSTORE`, `JSON`, `JSONB`, `DATERANGE`, `INT4RANGE`, `INT8RANGE`, `NUMRANGE`, `POINT`, `TSRANGE`, `XML`, `INET`, `CIDR`, `MACADDR`, `OID`, `REST`... | any |

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

    To configure a target {{ PG }} endpoint, provide the following settings:

    1. Configure the connection:

        1. Under **Connection settings** → **Connection type**,
            expand **Hosts**.

        1. Click ![plus icon](../_assets/plus-sign.svg) 
            and enter the host's IP address or domain name (FQDN).
            To add several hosts, repeat this step.

        1. In **Port**, enter the port for connection, or leave the default value of `5432`.

        1. To encrypt data transmission, upload a `.pem` file with certificates under **CA Certificate**.

        1. In **Database**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

            {% note warning %}

            The user must have a role with the `REPLICATION` attribute or be a superuser.
            [Learn more ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/logical-replication-security.html)

            {% endnote %}

        {% cut "Standard {{ PG }} connection string reference" %}

        Typically, a {{ PG }} connection string looks as follows:

        ```shell
        postgres://<username>:<password>@<hostname>:<port>/<database>
        ```

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<port>`: Port. Default is `5432`.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

    1. Select a **Cleanup policy** to specify
        how data in the target database is cleaned up when a transfer is activated, reactivated, or reloaded.

        {% cut "Cleanup policy reference" %}

        * **Disabled**:
            Don’t clean. Select this option if you only perform replication without copying data.

        * **Drop** (default):
            Fully delete the tables included in the transfer.
            Use this option
            to always transfer the latest version of the table schema to the target database from the source.

        * **Truncate**:
            Execute the
            [TRUNCATE ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/sql-truncate.html)
            command for the target table each time you run a transfer.

        {% endcut %}

    1. If you want to transfer all the transactions with context in the source database to the target database,
        expand **Advanced settings** and enable **Save transaction boundaries**.

        {% note info %}

        This feature is in the **Preview** stage.

        {% endnote %}

* Target data type mapping

    | **{{ data-transfer-name }} type** | **{{ PG }} type** |
    |---|---|
    |int64|`BIGINT`|
    |int32|`INTEGER`|
    |int16|`SMALLINT`|
    |int8|`SMALLINT`|
    |uint64|`BIGINT`|
    |uint32|`INTEGER`|
    |uint16|`SMALLINT`|
    |uint8|`SMALLINT`|
    |float|`REAL`|
    |double|`DOUBLE PRECISION`|
    |string|`BYTEA`|
    |utf8|`TEXT`|
    |boolean|`BOOLEAN`|
    |date|`DATE`|
    |datetime|`TIMESTAMP WITHOUT TIME ZONE`|
    |timestamp|`TIMESTAMP WITHOUT TIME ZONE`|
    |any|`JSONB`|

{% endlist %}
