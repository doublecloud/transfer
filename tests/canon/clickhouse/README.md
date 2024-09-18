### local run

Start docker for server:
```shell
docker run -d --name some-clickhouse-server  -e CLICKHOUSE_USER=default -e CLICKHOUSE_DB=canon -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -p 18123:8123 -p 19000:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

pipe snapshot dump into client
```shell
cat transfer_manager/go/tests/canon/clickhouse/snapshot/data.sql | clickhouse-client --host localhost --port 19000 --user default --multiline --multiquery
```

Pass env variables to test:

```shell
RECIPE_CLICKHOUSE_HTTP_PORT=18123 RECIPE_CLICKHOUSE_NATIVE_PORT=19000
```

Enjoy
