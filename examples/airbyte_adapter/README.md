## Airbyte provider

This is a bridge between native transfer and airbyte connector.
This adapter is ideal for scenarios where you need to synchronize data from an Airbyte-compatible source to a Transfer-compatible sink with minimal configuration.

We support source airbyte [connectors](https://docs.airbyte.com/category/sources)

This adapter enables integration between [Airbyte](https://docs.airbyte.com/using-airbyte/core-concepts/) and [Transfer](https://github.com/doublecloud/transfer), facilitating the translation of Airbyte's core concepts into Transfer-compatible constructs for streamlined data movement and transformations.


This example showcase how to integrate data from [Github](https://airbyte.com/connectors/github) to Clickhouse via Airbyte Connector.

## Overview

1. **Github Connector**: An [airbyte](https://docs.airbyte.com/integrations/sources/github) github api connector.
    - **PAM**: Personal access token to access to transfer opensource repo

3. **Transfer CLI**: A Go-based application that load API data from github to Clickhouse.

4. **Clickhouse**: An open source big data platform for distributed storage and processing.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine.
- Personal access token, see [here](https://github.com/settings/tokens)

### Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/doublecloud/transfer
   cd transfer/examples/mysql2ch
   ```
   
2. **Build and Run the Docker Compose**:
   ```bash
   export MY_TOKEN=TOKEN_VALUE
   docker-compose up --build
   ```

3. **Access to Clickhouse**:
   Access to ClickHouse via CLI:
   ```bash
   clickhouse-client --host localhost --port 9000 --user default --password 'ch_password'
   ```

### Configuration Files

- **`transfer.yaml`**: Specifies the source (Github Airbyte) and destination (CH) settings inside docker-compose

### Exploring results

Once docker compose up and running your can explore results via clickhouse-cli


```sql

SELECT *
FROM users
WHERE __data_transfer_delete_time = 0
   LIMIT 10


┌───id─┬─email──────────────────┬─name────┬─__data_transfer_commit_time─┬─__data_transfer_delete_time─┐
│ 3269 │ updated760@example.com │ User451 │         1732118484000000000 │                           0 │
│ 3281 │ updated646@example.com │ User91  │         1732118486000000000 │                           0 │
│ 3303 │ updated89@example.com  │ User107 │         1732118485000000000 │                           0 │
│ 3332 │ updated907@example.com │ User7   │         1732118485000000000 │                           0 │
│ 3336 │ updated712@example.com │ User473 │         1732118485000000000 │                           0 │
│ 3338 │ updated993@example.com │ User894 │         1732118485000000000 │                           0 │
│ 3340 │ updated373@example.com │ User313 │         1732118484000000000 │                           0 │
│ 3347 │ updated994@example.com │ User589 │         1732118484000000000 │                           0 │
│ 3348 │ updated515@example.com │ User96  │         1732118484000000000 │                           0 │
│ 3354 │ updated35@example.com  │ User267 │         1732118485000000000 │                           0 │
└──────┴────────────────────────┴─────────┴─────────────────────────────┴─────────────────────────────┘
```

### Stopping the Application

To stop the Docker containers, run:

```bash
docker-compose down
```

## Conclusion

This example provides a complete end-to-end Ingestion Solution using Github API, Clickhouse, and a Transfer application. You can use it to demonstrate how data can be replicated from Unstructured API Source to a Clickhouse data platform for real-time processing.
