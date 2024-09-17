## Type System Definition for MySQL


### MySQL Source Type Mapping

| MySQL TYPES | TRANSFER TYPE |
| --- | ----------- |
|BIGINT|int64|
|INT<br/>MEDIUMINT|int32|
|SMALLINT|int16|
|TINYINT|int8|
|BIGINT UNSIGNED|uint64|
|INT UNSIGNED<br/>MEDIUMINT UNSIGNED|uint32|
|SMALLINT UNSIGNED|uint16|
|TINYINT UNSIGNED|uint8|
|—|float|
|DECIMAL<br/>DECIMAL UNSIGNED<br/>DOUBLE<br/>FLOAT<br/>FLOAT UNSIGNED|double|
|BINARY<br/>BIT<br/>BLOB<br/>GEOMCOLLECTION<br/>GEOMETRY<br/>LINESTRING<br/>LONGBLOB<br/>MEDIUMBLOB<br/>MULTILINESTRING<br/>MULTIPOINT<br/>MULTIPOLYGON<br/>POINT<br/>POLYGON<br/>REST...<br/>TINYBLOB<br/>VARBINARY|string|
|CHAR<br/>ENUM<br/>LONGTEXT<br/>MEDIUMTEXT<br/>SET<br/>TEXT<br/>TIME<br/>TINYTEXT<br/>VARCHAR<br/>YEAR|utf8|
|—|boolean|
|DATE|date|
|—|datetime|
|DATETIME<br/>TIMESTAMP|timestamp|
|JSON|any|



### MySQL Target Type Mapping

| TRANSFER TYPE | MySQL TYPES |
| --- | ----------- |
|int64|BIGINT|
|int32|INT|
|int16|SMALLINT|
|int8|TINYINT|
|uint64|BIGINT|
|uint32|INT|
|uint16|SMALLINT|
|uint8|TINYINT|
|float|FLOAT|
|double|FLOAT|
|string|TEXT|
|utf8|TEXT|
|boolean|BIT|
|date|DATE|
|datetime|TIMESTAMP|
|timestamp|TIMESTAMP|
|any|JSON|
