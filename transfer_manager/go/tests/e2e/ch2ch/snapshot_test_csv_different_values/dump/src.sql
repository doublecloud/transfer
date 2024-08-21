CREATE DATABASE IF NOT EXISTS some_db;

CREATE TABLE some_db.some_table
(
    `StringVal`            String,
    `DateVal`              Date,
    `OneMoreStringVal`     String,
    `DateTimeVal`          DateTime,
    `StringWithDefaultVal` String DEFAULT 'misc',
    `NullableStringVal` Nullable(String),
    `UInt8Val`             UInt8,
    `Int16Val`             Int16,
    `Int32Val`             Int32,
    `UInt64Val`            UInt64,
    `Enum8Val` Enum8('false' = 0, 'true' = 1)
)
    ENGINE = MergeTree()
        PARTITION BY toMonday(DateVal)
        ORDER BY (StringVal, DateVal, OneMoreStringVal)
        SAMPLE BY OneMoreStringVal
        SETTINGS index_granularity = 8192;

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES ('Death is a natural part of life. Rejoice for those around you who transform into the Force. Mourn them do not. Miss them do not. Attachment leads to jealously. The shadow of greed, that is.',  '2019-01-01', 'simple string', 1546300800, 'qwe', null, 12, 1234, -4321, 123123, 'false');

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES ('ab', '2019-01-02', 'bc', 1546300800, 'cd', 'de', 12, 34, 56, 78, 'true');

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES (';', '2019-01-03', ';', 1546300800, ';', ';', 12, 34, 56, 78, 'false')
;

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES ('","', '2019-01-04', '""', 1546300800, '"', ';', 12, 34, 56, 78, 'true');

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES ('"""', '2019-01-05', '"', 1546300800, '"""', '\n', 12, 34, 56, 78, 'false');

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES ('""asd"a', '2019-01-06', 's,"ada', 1546300800, '"adads"er"', '\\n', 12, 34, 56, 78, 'true');

INSERT INTO some_db.some_table
(`StringVal`, `DateVal`, `OneMoreStringVal`, `DateTimeVal`, `StringWithDefaultVal`,
 `NullableStringVal`, `UInt8Val`, `Int16Val`, `Int32Val`, `UInt64Val`, `Enum8Val`)
VALUES ('""klaz-klaz', '2019-01-07', '{PO{PI^D&CV,"()', 1546300800, '""_)&*^(&%#^%#@', '\\t\\t', 12, 34, 56, 78, 'true');

SET format_csv_delimiter = ';'
