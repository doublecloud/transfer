CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.dt64 (
    val DateTime64(6)
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE test.decimal (
    val Decimal32(2)
)
ENGINE = MergeTree
ORDER BY tuple();
