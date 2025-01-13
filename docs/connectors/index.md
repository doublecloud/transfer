---
title: "Transfer connectors"
description: "Explore the list of {{ data-transfer-name }} connectors in {{ DC }} and see their usage in different transfer types."
---

# Transfer connectors

{% note info %}

{{ DC }} {{ data-transfer-name }} uses custom-built connectors for the following sources:
[{{ CH }}](clickhouse.md),
[{{ PG }}](postgresql.md),
[{{ MY }}](mysql.md),
[{{ MG }}](mongodb.md),
and [{{ S3 }}](object-storage.md).

Other connectors are based on [Airbyte](https://docs.airbyte.com/integrations/).

{% endnote %}

## Available Transfer connectors

| **Name**                    | **Type**              |
|:----------------------------|:----------------------|
| [{#T}](object-storage.md) | {{ DC }} custom-built |
| [{#T}](kafka.md)          | {{ DC }} custom-built |
| [{#T}](clickhouse.md)     | {{ DC }} custom-built |
| [{#T}](elasticsearch.md)  | {{ DC }} custom-built |
| [{#T}](mongodb.md)        | {{ DC }} custom-built |
| [{#T}](mysql.md)          | {{ DC }} custom-built |
| [{#T}](opensearch.md)     | {{ DC }} custom-built |
| [{#T}](postgresql.md)     | {{ DC }} custom-built |
| [{#T}](kinesis.md)        | {{ DC }} custom-built |

