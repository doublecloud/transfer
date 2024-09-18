## Type System Definition for ClickHouse


### ClickHouse Source Type Mapping

| ClickHouse TYPES | TRANSFER TYPE |
| --- | ----------- |
|Int64|int64|
|Int32|int32|
|Int16|int16|
|Int8|int8|
|UInt64|uint64|
|UInt32|uint32|
|UInt16|uint16|
|UInt8|uint8|
|—|float|
|Float64|double|
|FixedString<br/>String|string|
|Enum16<br/>Enum8<br/>IPv4<br/>IPv6|utf8|
|—|boolean|
|Date|date|
|DateTime|datetime|
|DateTime64|timestamp|
|REST...|any|



### ClickHouse Target Type Mapping

| TRANSFER TYPE | ClickHouse TYPES |
| --- | ----------- |
|int64|Int64|
|int32|Int32|
|int16|Int16|
|int8|Int8|
|uint64|UInt64|
|uint32|UInt32|
|uint16|UInt16|
|uint8|UInt8|
|float|Float64|
|double|Float64|
|string|String|
|utf8|String|
|boolean|UInt8|
|date|Date|
|datetime|DateTime|
|timestamp|DateTime64(9)|
|any|String|
