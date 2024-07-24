CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.test (
    arr Array(UInt32),
    arr_n Array(Nullable(UInt32)),
    arr_arr_int Array(Array(Int32)),
    arr_arr_str Array(Array(String))
) ENGINE = MergeTree
ORDER BY tuple();
