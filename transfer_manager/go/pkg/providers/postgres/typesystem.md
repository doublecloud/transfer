## Type System Definition for PostgreSQL


### PostgreSQL Source Type Mapping

| PostgreSQL TYPES | TRANSFER TYPE |
| --- | ----------- |
|BIGINT|int64|
|INTEGER|int32|
|SMALLINT|int16|
|—|int8|
|—|uint64|
|—|uint32|
|—|uint16|
|—|uint8|
|—|float|
|DOUBLE PRECISION<br/>NUMERIC<br/>REAL|double|
|BIT<br/>BIT VARYING<br/>BIT VARYING(N)<br/>BIT(N)<br/>BYTEA|string|
|ABSTIME<br/>CHAR<br/>CHARACTER VARYING<br/>DATA<br/>INTERVAL<br/>MONEY<br/>NAME<br/>TEXT<br/>TIME WITH TIME ZONE<br/>TIME WITHOUT TIME ZONE<br/>UUID|utf8|
|BOOLEAN|boolean|
|DATE|date|
|—|datetime|
|TIMESTAMP WITH TIME ZONE<br/>TIMESTAMP WITHOUT TIME ZONE|timestamp|
|ARRAY<br/>CHARACTER(N)<br/>CIDR<br/>CITEXT<br/>DATERANGE<br/>HSTORE<br/>INET<br/>INT4RANGE<br/>INT8RANGE<br/>JSON<br/>JSONB<br/>MACADDR<br/>NUMRANGE<br/>OID<br/>POINT<br/>REST...<br/>TSRANGE<br/>TSTZRANGE<br/>XML|any|



### PostgreSQL Target Type Mapping

| TRANSFER TYPE | PostgreSQL TYPES |
| --- | ----------- |
|int64|BIGINT|
|int32|INTEGER|
|int16|SMALLINT|
|int8|SMALLINT|
|uint64|BIGINT|
|uint32|INTEGER|
|uint16|SMALLINT|
|uint8|SMALLINT|
|float|REAL|
|double|DOUBLE PRECISION|
|string|BYTEA|
|utf8|TEXT|
|boolean|BOOLEAN|
|date|DATE|
|datetime|TIMESTAMP WITHOUT TIME ZONE|
|timestamp|TIMESTAMP WITHOUT TIME ZONE|
|any|JSONB|
