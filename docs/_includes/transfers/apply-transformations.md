{% include notitle [public-preview-notice](../administration/public-preview-notice.md) %}

For example, you don't want to transfer the column with passwords.
In **Transformer list**, click **Add transformer** to create a set of transformation rules.
Add this column's name to the `Exclude columns` section.
In **Transformer[number]** → **Columns filter**, configure transformations for tables and columns:

* **Include columns** sets the list of columns to transfer and **Exclude columns** makes the list of columns that won't be transferred. Set these table names as regular expressions.

Each transformer is a separate set of rules, and you can combine different rules within each set.

For example, you can set a table in `Include tables` and an `Exclude column`. In this case, the service will ignore the specified `Exclude columns` only for the included table. If you combine `Exclude tables` and `Include columns`, only the specified columns will be transferred from all tables except those specified in the`Exclude tables` field.

1. Click **+ Transformation** to add a new transformation layer. You can apply multiple layers to your data.

1. From the dropdown menu, select the appropriate transformation type:

   {% cut "Mask secret fields" %}

   This transformation allows you to apply a hash function to specified columns in tables to further protect sensitive data during transfer.

   1. Under **Tables**, specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Column list**, click **+** to add a column name. The masking will be applied to the columns listed in this section.

   1. Under **Mask function** → **Hash** → **User-defined Salt**, specify the [Salt hash ![external link](../_assets/external-link.svg)](https://doubleoctopus.com/security-wiki/encryption-and-cryptography/salted-secure-hash-algorithm/) you want to apply to your data.

   {% endcut %}

   {% cut "Columns filter" %}

   This transformation allows you to apply filtering to the list of columns to transfer from the data source.

   1. Under **Tables**, click **+ Tables** and specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Columns**, specify the following:

      * **Included columns** restricts the set of columns to transfer from the tables specified above.

      * **Excluded columns** allow transferring all columns except the specified ones.

      Set these column names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   {% endcut %}

   {% cut "Rename tables" %}

   This transformation gives you a capability to associate the table name on the source with a new table name on the target without changing the contents of the transferred table.

   1. Under **Tables list to rename**, click **+ Table**.

   1. Under **Table 1** → **Source table name**:

      * For {{ PG }} data sources, use the **Named schema** field to provide the source table schema. Leave empty for the data sources that don't support schema and/or database abstractions.

      * Specify the initial **Table name** on the source.

   1. Under **Target table name**:

      * For {{ PG }} data sources, use the **Named schema** field to provide the target table schema. Leave empty for the data sources that don't support schema and/or database abstractions.

      * Specify the intended **Table name** on the target.

   {% endcut %}

   {% cut "Replace primary key" %}

   This transformation allows you to reassign the primary key column on the target table.

   1. Under **Tables**, click **+ Tables** and specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

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

   1. Under **Tables**, click **+ Tables** and specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Columns**, specify the following:

      * **Included columns** restricts the set of columns to transfer from the tables specified above.

      * **Excluded columns** allow transferring all columns except the specified ones.

      Set these column names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Key columns names**, click **+** to add a name of the column containing the primary key.

   1. Under **Non-key column names**, click **+** to add a column without a primary key.

   The conversion will be applied to the columns listed in both sections.

   {% endcut %}

   {% cut "Convert data to raw JSON" %}

   This transformation gives you a capability to convert a certain data column in a specified table to a raw JSON.

   1. Under **Tables**, specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Columns**, specify the following:

      * **Included columns** restricts the set of columns to transfer from the tables specified above.

      * **Excluded columns** allow transferring all columns except the specified ones.

      Set these column names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Key columns names**, click **+** to add a name of the column containing the primary key.

   1. Under **Non-key column names**, click **+** to add a column without a primary key.

   The conversion will be applied to the columns listed in both sections.

   {% endcut %}

   {% cut "Sharding" %}

   This transformation allows you to distribute the tables between multiple [shards](../../managed-clickhouse/concepts/sharding.md) on the {{ CH }} data destination.

   1. Under **Tables**, click **+ Tables** and specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Columns**, click **+ Columns** and specify the following:

      * **Included columns** restricts the set of columns to transfer from the tables specified above.

      * **Excluded columns** allow transferring all columns except the specified ones.

   1. Enter the **Count of shards** between which you want to distribute the table data.

   {% endcut %}

   {% cut "Convert CDC data to raw JSON with history" %}

   This transformation allows you to convert the [change data capture (CDC) data ![external link](../_assets/external-link.svg)](https://en.wikipedia.org/wiki/Change_data_capture) to raw JSON with history.

   1. Under **Tables**, click **+ Tables** and specify the following:

      * **Included tables** restricts the set of tables to transfer.

      * **Excluded tables** allow transferring all data except the specified tables.

      Set these table names as regular expressions:

      {% include notitle [regular-expressions](../transfers/regular-expressions.md) %}

   1. Under **Key columns names**, click **+** to add a name of the column containing the primary key.

   1. Under **Non-key column names**, click **+** to add a column without a primary key.

   The conversion will be applied to the columns listed in both sections.

   {% endcut %}

   {% cut "SQL" %}

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

   {% endcut %}

   {% cut "dbt" %}

   Apply your dbt project to the snapshot of the data transferred to {{ CH }}.

   For more information on how to apply dbt transformation to your data, see [{#T}](../../managed-clickhouse/integrations/transform-data-in-clickhouse-with-dbt.md).

   1. Specify the address of the **Git repository** containing your dbt project. It must start with `https://`. The root directory of the repository must contain a `dbt_project.yml` file.

   1. Under **Git branch**, specify the branch or a tag of the git repository containing your dbt project.

   1. Provide the **DBT profile name** which will be created automatically using the settings of the destination endpoint. The name must match the `profile` property in the `dbt_project.yml` file.

   1. From the dropdown list, select the **Operation** for your dbt project to perform. For more information, see the [official dbt documentation ![external link](../_assets/external-link.svg)](https://docs.getdbt.com/docs/build/hooks-operations#about-operations).

   {% endcut %}

{% note tip "Deleting a transformation layer" %}

To delete a transformation layer, 
click ![three horizontal dots](../_assets/horizontal-ellipsis.svg)
to the right of the transformation type dropdown menu → **Delete** .

{% endnote %}
