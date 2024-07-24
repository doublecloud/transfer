## Type System Definition for Airbyte


### Airbyte Source Type Mapping

| Airbyte TYPES | TRANSFER TYPE |
| --- | ----------- |
|integer|int64|
|—|int32|
|—|int16|
|—|int8|
|—|uint64|
|—|uint32|
|—|uint16|
|—|uint8|
|—|float|
|number|double|
|—|string|
|string<br/>time_with_timezone<br/>time_without_timezone|utf8|
|boolean|boolean|
|date|date|
|date-time|datetime|
|timestamp<br/>timestamp_with_timezone<br/>timestamp_without_timezone|timestamp|
|REST...<br/>array<br/>object|any|


### Airbyte Target Type Mapping Not Specified
