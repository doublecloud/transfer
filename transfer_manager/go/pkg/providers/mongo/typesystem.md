## Type System Definition for MongoDB


### MongoDB Source Type Mapping

| MongoDB TYPES | TRANSFER TYPE |
| --- | ----------- |
|—|int64|
|—|int32|
|—|int16|
|—|int8|
|—|uint64|
|—|uint32|
|—|uint16|
|—|uint8|
|—|float|
|—|double|
|—|string|
|bson_id|utf8|
|—|boolean|
|—|date|
|—|datetime|
|—|timestamp|
|bson|any|



### MongoDB Target Type Mapping

| TRANSFER TYPE | MongoDB TYPES |
| --- | ----------- |
|int64|bson|
|int32|bson|
|int16|bson|
|int8|bson|
|uint64|bson|
|uint32|bson|
|uint16|bson|
|uint8|bson|
|float|bson|
|double|bson|
|string|bson|
|utf8|bson|
|boolean|bson|
|date|bson|
|datetime|bson|
|timestamp|bson|
|any|bson|
