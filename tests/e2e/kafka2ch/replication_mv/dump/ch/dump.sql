CREATE DATABASE public;

CREATE TABLE IF NOT EXISTS public.`topic1`
(
    `id` Nullable(Int32),
    `level` Nullable(String),
    `caller` Nullable(String),
    `msg` Nullable(String),
    `_timestamp` DateTime64(6),
    `_partition` String,
    `_offset` UInt64,
    `_idx` UInt32
)
ENGINE=MergeTree()
ORDER BY (`id`, `_timestamp`, `_partition`, `_offset`, `_idx`)
SETTINGS allow_nullable_key = 1;


CREATE TABLE public.__test_aggr
(
    `is_even` Int8,
    `sum_id` UInt64
)
    ENGINE = SummingMergeTree()
ORDER BY (is_even);

CREATE MATERIALIZED VIEW public.__test_mv
TO public.__test_aggr
AS
SELECT
    coalesce(id / 2, 0) is_even,
    sum(toInt32(_partition)) AS sumVal -- at replication we will try to insert null, it should fail sum
FROM public.topic1
GROUP BY
    is_even;
