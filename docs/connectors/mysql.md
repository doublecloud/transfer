---
title: "{{ MY }} connector"
description: "Configure the {{ MY }} connector to transfer data from {{ MY }} databases with {{ DC }} {{ data-transfer-name }}"

---

# {{ MY }} connector

You can use this connector both for **source** and **target** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings** → **Connection type**, specify the connection properties:

      * The **Host** IP address or FQDN to connect to.

      * The **Port** for connection (`3306` by default).

      * **CA certificate**. Click **Upload file** to upload a certificate file (public key) in PEM format.

   1. Specify the database attributes:

      * **Database** name associated with the username and password below.

      * **User** for connection to {{ data-transfer-name }}.

      * **Password** for the database user.

   1. Configure the **Table filter** if you need to transfer specific tables. If you want to transfer all the available tables, skip this section.

      * **Included Tables**. 
          {{ data-transfer-name }} will transfer only the data from these tables.
          Specify the table names together with the database in the `database_name.target_table` format.

      * **Excluded tables**. The data from the tables on this list won't be transferred. Specify the table names after the name of the database containing these tables as follows: `database_name.excluded_table`.

      {% note tip %}

      To parse multiple tables in the sections above, use regular expressions as conditions to parse the tables available at the source.

      {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

      You can add fields to the above sections to function as multiple parsers or filters.

      {% endnote %}

   1. Configure the **Transfer schema**:

      This section allows you explicitly select the database schema items to migrate and set the specific transfer stage at which to migrate them.

      {% note warning %}

      In most cases, the default schema migration settings let you perform a successful transfer. Change the settings for the initial and final stages of the transfer only if necessary.

      {% endnote %}

      The transfer is performed in two stages:

      * **Activation stage**

         This stage executes on activation of the transfer, before [snapshot](../concepts/transfer-types.md#snapshot) or replication to create schema on the target.

         You can select parts of the schema to be transferred at this stage.

         By default, the **Tables** are transferred at start.

      * **Deactivation stage**

         This stage executes at the end of the transfer upon its deactivation.

         If the transfer is constantly working in replication mode ([Replication](../concepts/transfer-types.md) and [Snapshot and replication](../concepts/transfer-types.md) transfer types), the final stage of the transfer will be performed only after the replication stops. You can choose which parts of the schema to migrate.

         At this stage, it's assumed that when the transfer is deactivated, there is no writing activity on the source. For complete reliability, the source is set to read-only mode. The database schema on the target is brought to a state where it will be consistent with the schema on the source.

      {% note info %}

      When the transfer is restarted during the replication phase, the table schemas on the target are preserved. In this case, the service will transfer only the table schemas unavailable at the target after the restart.

      {% endnote %}

   1. Under **Advanced settings**:

      * **Database timezone**. Specified as [IANA Time Zone Database ![external link](../_assets/external-link.svg)](https://www.iana.org/time-zones) identifier. The default timezone is `Local`, it corresponds to the **{{ MY }}** server timezone.

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings** → **Connection type**, specify the connection properties:

      * **IP or FQDN of the host** to connect to.

      * The **Port** for connection (`3306` by default).

      * **CA certificate**. Click **Choose a file** to upload a certificate file (public key) in PEM format or provide it as text.

      1. Specify the database attributes:

         * **Database** name associated with the username and password below.

         * **Username** for connecting to {{ data-transfer-name }}.

         * **Password** for the database user.

   1. Select a **Cleanup policy**. This policy allows you to select a way to clean up data in the target database when you activate, reactivate or reload the transfer:

      * `Don't cleanup`: Do not clean. Select this option if you only perform replication without copying data.

      * `Drop`: Fully delete the tables included in the transfer (default). Use this option to always transfer the latest version of the table schema to the target database from the source.

      * `Truncate`: Execute the [TRUNCATE ![external link](../_assets/external-link.svg)](https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html) command for a target table each time you run a transfer.

   1. Specify **Advanced settings**:

      * **Database timezone**. Specified as [IANA Time Zone Database ![external link](../_assets/external-link.svg)](https://www.iana.org/time-zones) identifier. You can also set the special `Local` timezone as a string. This timezone corresponds to the **MySQL** server timezone. The default timezone is `Local`.

      * **SQL modes** [enabled on the target server ![external link](../_assets/external-link.svg)](https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html). The delimiter for multiple modes is the comma (`,`).

         The default modes are the following:

         ```sql
         NO_AUTO_VALUE_ON_ZERO,NO_DIR_IN_CREATE,NO_ENGINE_SUBSTITUTION
         ```

      * Check **Disable constraints checks** if you don't need [these checks ![external link](../_assets/external-link.svg)](https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html) and want to speed up the replication.

      * The **Database schema for service tables** specifies the database into which to place the tables with the service information.

{% endlist %}
