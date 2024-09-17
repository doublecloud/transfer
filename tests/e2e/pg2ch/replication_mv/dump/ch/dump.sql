CREATE DATABASE public;

CREATE TABLE IF NOT EXISTS public.__test
(
    `id` Int32,
    `val1` Nullable(Int32),
    `val2` Nullable(String),
    `__data_transfer_commit_time` UInt64,
    `__data_transfer_delete_time` UInt64
    )
    ENGINE = ReplacingMergeTree(__data_transfer_commit_time)
    ORDER BY id;

CREATE TABLE public.__test_aggr
(
    `is_even` Int8,
    `sumVal` UInt64
)
    ENGINE = SummingMergeTree()
ORDER BY (is_even);

CREATE MATERIALIZED VIEW public.__test_mv
TO public.__test_aggr
AS
SELECT
    coalesce(val1 / 2, 0) is_even,
    sum(val1) AS sumVal -- at replication we will try to insert null, it should fail sum
FROM public.__test
GROUP BY
    is_even;
