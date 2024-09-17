CREATE DATABASE IF NOT EXISTS db;


CREATE TABLE db.test_table
(
    `Id` UInt16,
    `Name` String,
    `Age` UInt16,
    `Birthday` Date
)
    ENGINE = MergeTree()
ORDER BY (Age);

INSERT INTO db.test_table
(`Id`, `Name`, `Age`, `Birthday`)
VALUES
(1, 'Bob', 20, '2019-01-01'), (2, 'Gwen', 25, '2019-01-02'), (3, 'John', 45, '2019-01-03')
;
