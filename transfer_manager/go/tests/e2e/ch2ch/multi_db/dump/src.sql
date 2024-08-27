CREATE DATABASE IF NOT EXISTS db1;

CREATE TABLE db1.long_line
(
    `id` UInt64,
    `value` String
)
ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 8192;

INSERT INTO db1.long_line
(`id`, `value`)
VALUES
(1, repeat('a', 5000))
;

CREATE DATABASE IF NOT EXISTS db2;

CREATE TABLE db2.long_line
(
    `id` UInt64,
    `value` String
)
    ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 8192;

INSERT INTO db1.long_line
(`id`, `value`)
VALUES
    (1, repeat('b', 5000))
;


CREATE DATABASE IF NOT EXISTS db3;

CREATE TABLE db3.long_line
(
    `id` UInt64,
    `value` String
)
    ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 8192;

INSERT INTO db1.long_line
(`id`, `value`)
VALUES
    (1, repeat('c', 5000))
;
