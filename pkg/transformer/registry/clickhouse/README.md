## Clickhouse transformer

Based on [Clickhouse Local](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/)

This tool accept clickhouse SQL dialect and allow to produce SQL-like in memory data transformation.

![diagram](https://jing.yandex-team.ru/files/tserakhau/ch_local_trans.svg)

Source table inside CH Local named as `table`, clickhouse table structure mimic source table structure.

Since each source change item (row) contains extra metadata we must match source and target data together.
There for each row must have a key defined.
All of this key should be uniq in every batch (for this we call collapse function).
If we can't match source keys with transformed data we will mark such row as errored.
