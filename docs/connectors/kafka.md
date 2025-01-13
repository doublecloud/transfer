---
title: "{{ KF }} connector"
description: "Configure the {{ KF }} connector to transfer data from {{ KF }} with {{ DC }} {{ data-transfer-name }}"

---

# {{ KF }} connector

You can use this connector both for **source** and **target** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings**, select the **Connection type**:

      {% cut "Managed cluster" %}

      1. Select your **Managed cluster** from the drop-down list.

      1. Select the **Authentication** method:

         * To verify your identity with **SASL**, click **+ SASL**, and specify the credentials:

            * Your **User Name**,

            * **Password** for this user name,

         * Set this property to **No Authentication** if you don't need authentication.

      {% endcut %}

      {% cut "On-premise" %}

      1. Under **Broker URLs**, click **+ URL** to add brokers.

         Enter IP address or domain names (FQDN) of broker hosts.

         {% note tip %}

         If the {{ KF }} port number isn't standard, separate it with a colon as follows:

         `<Broker host IP of FQDN>:<port number>`

         {% endnote %}

      1. Click **SSL** if you want to encrypt your connection.

      1. Add the **PEM Certificate**. Click **Choose a file** to upload a certificate file (public key) in PEM format or provide it as text.

      1. Select the **Authentication** method:

         1. To verify your identity with **SASL**, specify the following options:

            * Your **User Name**,

            * **Password** for this user name,

            * Encryption **Mechanism**.

         1. Set this property to **No Authentication** if you don't need authentication.

      {% endcut %}

   1. Specify the **Topic full name**.

   1. Configure **Advanced settings** → **Conversion rules**:

      1. Click **+ Conversion rules**.

      1. Select the **Data format**. Currently, we support the `JSON` format.

      1. Choose the **Data scheme**:

         {% cut "Field list" %}

         1. Under **Field list** → **Field** click **Add Field** and specify the field properties:

         1. The **name** of the field.

         1. Select the field **type**.

         1. (optional) Check **Key** to make the field a table sorting key.

         1. (optional) Check the **Required** box to make the field obligatory.

         1. Provide the **Path** to redefine the names of columns in the table following

            ```sh
            library.shelf[a].book[b].title`
            ```

         {% endcut %}

         {% cut "JSON Spec" %}

         Click **Choose a file** to provide a file with schema description in JSON format. The schema should look as follows:

         ```json
         [
            {
               "name": "remote_addr",
               "type": "string"
            },
            {
               "name": "remote_user",
               "type": "string"
            },
            {
               "name": "time_local",
               "type": "string"
            },
            {
               "name": "request",
               "type": "string"
            },
            {
               "name": "status",
               "type": "int32"
            },
            {
               "name": "bytes_sent",
               "type": "int32"
            },
            {
               "name": "http_referer",
               "type": "string"
            },
            {
               "name": "http_user_agent",
               "type": "string"
            }
         ]
         ```

         {% endcut %}

      1. Check the **Add a column for missing keys** box if you need to collect keys missing from the scheme.

      1. Check **Enable null values in keys** if needed.

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings**, select the **Connection type**:

      {% cut "Managed cluster" %}

      1. Select your **Managed {{ KF }} cluster** from the drop-down list.

      1. Select the **Authentication** method:

         * To verify your identity with SASL, click **+ SASL**, and specify the credentials:

            * Your **User Name**,

            * **Password** for this user name.

         * Set this property to **No Authentication** if you don't need to provide credentials.

      {% endcut %}

      {% cut "On-premise cluster" %}

      1. Provide **Broker URLs**:

      1. Click **Broker URLs** → **+ URL** to add brokers.

         Enter the IP addresses or domain names (FQDN) of broker hosts.

         {% note tip %}

         If the {{ KF }} port number isn't standard, separate it with a colon as follows:

         ```sh
         <Broker host IP of FQDN>:<port number>
         ```

         {% endnote %}

      1. Click **SSL** if you want to encrypt your connection.

      1. Add the **PEM Certificate**. Click **Choose a file** to upload a certificate file (public key) in PEM format or paste it as text.

      1. Select the **Authentication** method:

         1. To verify your identity with **SASL**, specify the following options:

            * Your **User Name**,

            * **Password** for this user name,

            * Encryption **Mechanism**.

         1. Set this property to **No Authentication** if you don't need to provide credentials.

      {% endcut %}

   1. Specify the **Apache Kafka topic settings**:

      {% cut "Topic full name" %}

      1. Specify the **Topic full name** in the target cluster as a full path.

      {% endcut %}

      {% cut "Topic prefix" %}

      1. Specify the **Topic prefix** in the target cluster. The format is the following: `topic_prefix.schema.table_name`, it's similar to the [Debezium ![external link](../_assets/external-link.svg)](https://debezium.io/) settings.

      {% endcut %}

      To parse multiple topic names in the above sections, you can use regular expressions:

      {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

   1. Configure **Advanced settings**:

      1. Check the **Save tx order** box if you want to write database transactions in the same order.

         When you enable this setting, the service writes all tables from the source database into a single [partition ![external link](../_assets/external-link.svg)](https://kafka.apache.org/documentation/#intro_concepts_and_terms). The tables order is preserved.

         In the default mode, when this setting is disabled, the service splits the data you transfer by table names. Each table goes to a separate partition.

         {% note warning %}

         This setting applies only if a transfer meets both conditions below:

         * The source endpoint is [{{ PG }}](postgresql.md#source-endpoint-configuration) or [{{ MY }}](mysql.md#source-endpoint-configuration).

         * You set `Topic prefix` in **Apache Kafka topic settings**.

         {% endnote %}

{% endlist %}
