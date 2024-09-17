## Type System Definition for Delta Lake


### Delta Lake Source Type Mapping

| Delta Lake TYPES | TRANSFER TYPE |
| --- | ----------- |
|bigint<br/>long|int64|
|int<br/>integer|int32|
|short<br/>smallint|int16|
|byte<br/>tinyint|int8|
|—|uint64|
|—|uint32|
|—|uint16|
|—|uint8|
|double|float|
|float<br/>real|double|
|binary|string|
|string|utf8|
|boolean|boolean|
|date|date|
|—|datetime|
|timestamp|timestamp|
|REST...|any|


### Delta Lake Target Type Mapping Not Specified
