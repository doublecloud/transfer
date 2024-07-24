CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.test (
    c_int16 Int16,
    c_int32 Nullable(Int32),
    c_int64 Int64,
    c_uint32 Nullable(UInt32),
    c_uint64 UInt64
) ENGINE = MergeTree
ORDER BY tuple();
