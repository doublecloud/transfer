---
title: "{{ MG }} connector"
description: "Configure the {{ MG }} connector to transfer data to and from {{ MG }} databases with {{ DC }} {{ data-transfer-name }}"
---

# {{ MG }} connector

You can use the {{ MG }} connector in both **source** and **target** endpoints.
In source endpoints, the connector retrieves data from {{ MG }} databases.
In target endpoints, it inserts data to {{ MG }} databases.

## Source endpoint

{% list tabs %}

* Configuration

    To configure a {{ MG }} source endpoint, take the following steps:

    1. In **Connection type**,
        select whether you want to connect to {{ MG }} with the standard connection (**Custom installation**)
        or with **SRV**.

        * **Custom installation** uses the standard connection method,
            and you need to specify all hosts and ports.

        * **SRV** provides a simplified way to connect to {{ MG }} clusters,
            in particular those on {{ MG }} Atlas.

    1. Configure the connection:

        {% cut "Custom installation" %}

        1. Under **Host**,
             click **+ Host** and enter the domain name (FQDN) or IP address of the host.

        1. In **Replica set**, enter the replica set ID.

        1. In **Port**, enter the port to connect with.

        1. To encrypt data transmission,
            upload a `.pem` file with the certificate (public key) under **CA Certificate**.

        1. In **Authentication source**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        {% cut "Standard {{ MG }} connection string reference" %}

        Typically, a {{ MG }} connection string looks as follows:

        ```shell
        mongodb://<username>:<password>@<hostname>:<port>/<database>
        ```

        * `mongodb://`: Standard {{ MG }} protocol.
            If your connection string starts with `mongodb+srv://`,
            use the **SRV** connection type instead.

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<port>`: Port.
            Default is `27018` for non-sharded clusters and `27017` for sharded ones.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

        {% endcut %}

        {% cut "SRV" %}

        1. Under **Hostname**, enter the domain name (FQDN) or IP address of the host.

            When you use the SRV connection format, you don’t need to specify the port.

        1. In **Replica set**, enter the replica set ID.

        1. To encrypt data transmission, upload a `.pem` file with the certificate (public key) under **CA Certificate**.

        1. In **Authentication source**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        {% cut "SRV connection string reference" %}

         ```shell
        mongodb+srv://<username>:<password>@<hostname>/<database>
        ```

        * `mongodb+srv://`: Prefix for the SRV connection format.

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

        {% endcut %}

    1. Under **Collection filter**, specify collections and tables you want to include or exclude:

        * **Included collections**: {{ data-transfer-name }} will transfer data only from these collections.

        * **Excluded collections**: Data from these collections won’t be transferred.

* Source data type mapping

   | **{{ MG }} type** | **{{ data-transfer-name }} type** |
   |---|---|
   |—|[int64](*int64)|
   |—|[int32](*int32)|
   |—|[int16](*int16)|
   |—|[int8](*int8)|
   |—|[uint64](*uint64)|
   |—|[uint32](*uint32)|
   |—|[uint16](*uint16)|
   |—|[uint8](*uint8)|
   |—|[float](*float)|
   |—|[double](*double) |
   |—|[string](*string)|
   |`bson`|[utf8](*utf8)|
   |—|[boolean](*bool)|
   |—|[date](*date)|
   |—|[datetime](*datetime)|
   |—|[timestamp](*timestamp)|
   |`bson`|any|

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

    To configure a target {{ MG }} endpoint, take the following steps:

    1. In **Connection type**,
        select whether you want to connect to {{ MG }} with the standard connection (**Custom installation**)
        or with **SRV**.

        * **Custom installation** uses the standard connection method,
            and you need to specify all hosts and ports.

        * **SRV** provides a simplified way to connect to {{ MG }} clusters,
            in particular those on {{ MG }} Atlas.

    1. Configure the connection:

        {% cut "Custom installation" %}

        1. Under **Host**,
            click **+ Host** and enter the domain name (FQDN) or IP address of the host.

        1. In **Replica set**, enter the replica set ID.

        1. In **Port**, enter the port to connect with.

        1. To encrypt data transmission,
            upload a `.pem` file with the certificate (public key) under **CA Certificate**.

        1. In **Authentication source**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        {% cut "Standard {{ MG }} connection string reference" %}

        Typically, a {{ MG }} connection string looks as follows:

        ```shell
        mongodb://<username>:<password>@<hostname>:<port>/<database>
        ```

        * `mongodb://`: Standard {{ MG }} protocol.
            If your connection string starts with `mongodb+srv://`,
            use the **SRV** connection type instead.

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<port>`: Port.
            Default is `27018` for non-sharded clusters and `27017` for sharded ones.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

        {% endcut %}

        {% cut "SRV" %}

        1. Under **Hostname**, enter the domain name (FQDN) or IP address of the host.

            When you use the SRV connection format, you don’t need to specify the port.

        1. In **Replica set**, enter the replica set ID.

        1. To encrypt data transmission,
            upload a `.pem` file with the certificate (public key) under **CA Certificate**.

        1. In **Authentication source**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        {% cut "SRV connection string reference" %}

         ```shell
        mongodb+srv://<username>:<password>@<hostname>/<database>
        ```

        * `mongodb+srv://`: Prefix for the SRV connection format.

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

        {% endcut %}

        1. In **Authentication source**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        1. In **Database name**, enter the name of the database where you want to transfer your data.

            {% note warning %}

            If you don’t specify a **database name**,
            the collections will be created in databases with the same names as in the source.

            {% endnote %}

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
                [remove() ![external link](../_assets/external-link.svg)](https://www.mongodb.com/docs/manual/reference/method/db.collection.remove/)
                command for the target table each time you run a transfer.

           {% endcut %}

* Target data type mapping

   | **{{ data-transfer-name }} type** | **{{ MG }} type** |
   |---|---|
   |int64|`bson`|
   |int32|`bson`|
   |int16|`bson`|
   |int8|`bson`|
   |uint64|`bson`|
   |uint32|`bson`|
   |uint16|`bson`|
   |uint8`bson`|
   |float|`bson`|
   |double|`bson`|
   |string|`bson`|
   |utf8|`bson`|
   |boolean|`bson`|
   |date|`bson`|
   |datetime|`bson`|
   |timestamp|`bson`|
   |any|`bson`|

{% endlist %}
