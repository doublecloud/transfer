CREATE DATABASE IF NOT EXISTS canon;

/*SET allow_experimental_object_type=1; -- Needs for JSON type */

CREATE TABLE canon.table
(
    id UInt64,

    /* All data types */

    col_Int8 Int8,
    col_Int16 Int16,
    col_Int32 Int32,
    col_Int64 Int64,
    /* col_Int128 Int128, -- Error: unhandled type Int128 */
    /* col_Int256 Int256, -- Error: unhandled type Int256 */

    col_UInt8 UInt8,
    col_UInt16 UInt16,
    col_UInt32 UInt32,
    col_UInt64 UInt64,
    /* col_UInt128 UInt128, -- Error: unhandled type UInt128 */
    /* col_UInt256 UInt256, -- Error: unhandled type UInt256 */

    col_Float32 Float32,
    col_Float64 Float64,

    col_Decimal32 Decimal32(2),
    col_Decimal64 Decimal64(3),
    col_Decimal128 Decimal128(4),
    col_Decimal256 Decimal256(5),

    /* col_Bool Bool, -- Error: unhandled type Bool */

    col_String String,

    col_FixedString_1 FixedString(32),
    col_FixedString_2 FixedString(64),

    col_UUID UUID,

    col_Date Date,
    /* col_Date32 Date32, -- Error: unhandled type Date32 */
    col_DateTime DateTime('UTC'),
    col_DateTime64_1 DateTime64(0, 'UTC'),
    col_DateTime64_2 DateTime64(3, 'UTC'),
    col_DateTime64_3 DateTime64(6, 'UTC'),

    col_enum_1 Enum('a' = -128, 'b' = 127),
    col_enum_2 Enum('a' = -32768, 'b' = 32767),

    /* col_JSON JSON, -- Error: unhandled type 'a Int8', unhandled type 'a String' */

    /* Nullable */

    col_n_Int8 Nullable(Int8),
    col_n_Int16 Nullable(Int16),
    col_n_Int32 Nullable(Int32),
    col_n_Int64 Nullable(Int64),
    /* col_n_Int128 Nullable(Int128), -- Error: unhandled type Int128 */
    /* col_n_Int256 Nullable(Int256), -- Error: unhandled type Int256 */

    col_n_UInt8 Nullable(UInt8),
    col_n_UInt16 Nullable(UInt16),
    col_n_UInt32 Nullable(UInt32),
    col_n_UInt64 Nullable(UInt64),
    /* col_n_UInt128 Nullable(UInt128), -- Error: unhandled type UInt128 */
    /* col_n_UInt256 Nullable(UInt256), -- Error: unhandled type UInt256 */

    col_n_Float32 Nullable(Float32),
    col_n_Float64 Nullable(Float64),

    col_n_Decimal32 Nullable(Decimal32(2)),
    col_n_Decimal64 Nullable(Decimal64(3)),
    /* col_n_Decimal128 Nullable(Decimal128(4)), -- Error: reflect: call of reflect.Value.Interface on zero Value */
    /* col_n_Decimal256 Nullable(Decimal256(5)), -- Error: Nullable(T): precision of Decimal exceeds max bound */

    /* col_n_Bool Nullable(Bool), -- Error: unhandled type Bool */

    col_n_String Nullable(String),

    col_n_FixedString_1 Nullable(FixedString(32)),
    col_n_FixedString_2 Nullable(FixedString(64)),

    col_n_UUID Nullable(UUID),

    col_n_Date Nullable(Date),
    /* col_n_Date32 Nullable(Date32), -- Error: unhandled type Date32 */
    col_n_DateTime Nullable(DateTime('UTC')),
    col_n_DateTime64_1 Nullable(DateTime64(0, 'UTC')),
    col_n_DateTime64_2 Nullable(DateTime64(3, 'UTC')),
    col_n_DateTime64_3 Nullable(DateTime64(6, 'UTC')),

    col_n_enum_1 Nullable(Enum('a' = -128, 'b' = 127)),
    col_n_enum_2 Nullable(Enum('a' = -32768, 'b' = 32767)),

    /* Arrays */

    col_arr_Int8 Array(Nullable(Int8)),
    col_arr_Int16 Array(Nullable(Int16)),
    col_arr_Int32 Array(Nullable(Int32)),
    col_arr_Int64 Array(Nullable(Int64)),
    /* col_arr_Int128 Array(Nullable(Int128)), -- Error: unhandled type Int128 */
    /* col_arr_Int256 Array(Nullable(Int256)), -- Error: unhandled type Int256 */

    col_arr_UInt8 Array(Nullable(UInt8)),
    col_arr_UInt16 Array(Nullable(UInt16)),
    col_arr_UInt32 Array(Nullable(UInt32)),
    col_arr_UInt64 Array(Nullable(UInt64)),
    /* col_arr_UInt128 Array(Nullable(UInt128)), -- Error: unhandled type UInt128 */
    /* col_arr_UInt256 Array(Nullable(UInt256)), -- Error: unhandled type UInt256 */

    col_arr_Float32 Array(Nullable(Float32)),
    col_arr_Float64 Array(Nullable(Float64)),

    col_arr_Decimal32 Array(Nullable(Decimal32(2))),
    col_arr_Decimal64 Array(Nullable(Decimal64(3))),
    /* col_arr_Decimal128 Array(Nullable(Decimal128(4))), -- Error: unsupported Array type */
    /* col_arr_Decimal256 Array(Nullable(Decimal256(5))), -- Error: Array(T): Nullable(T): precision of Decimal exceeds max bound */

    /* col_arr_Bool Array(Nullable(Bool)), -- Error: unhandled type Bool */

    col_arr_String Array(Nullable(String)),

    col_arr_FixedString_1 Array(Nullable(FixedString(32))),
    col_arr_FixedString_2 Array(Nullable(FixedString(64))),

    col_arr_UUID Array(Nullable(UUID)),

    /*
    Arrays with dates/datetimes reads incorrectly, with strange timezones (+3h/+2.5h)
    col_arr_Date Array(Nullable(Date)),
    col_arr_Date32 Array(Nullable(Date32)), -- Error: unhandled type Date32
    col_arr_DateTime Array(Nullable(DateTime('UTC'))),
    col_arr_DateTime64_1 Array(Nullable(DateTime64(0, 'UTC'))),
    col_arr_DateTime64_2 Array(Nullable(DateTime64(3, 'UTC'))),
    col_arr_DateTime64_3 Array(Nullable(DateTime64(6, 'UTC'))),
    */

    col_arr_enum_1 Array(Nullable(Enum('a' = -128, 'b' = 127))),
    col_arr_enum_2 Array(Nullable(Enum('a' = -32768, 'b' = 32767)))

    /* Maps */

    /*
    Map types are not supported, error: unhandled type Map(<type>,<type>)
    col_map_string_n_string Map(String, Nullable(String)),
    col_map_int_n_int Map(Int32, Nullable(Int32)),
    col_map_fixedstring_n_fixedstring Map(FixedString(32), Nullable(FixedString(32))),
    col_map_uuid_n_uuid Map(UUID, Nullable(UUID)),
    col_map_date_n_date Map(Date, Nullable(Date)),
    col_map_datetime_n_datetime Map(DateTime('UTC'), Nullable(DateTime('UTC'))),
    col_map_date32_n_date32 Map(Date32, Nullable(Date32)),
    col_map_enum_n_enum Map(Enum('a' = -128, 'b' = 127), Nullable(Enum('a' = -128, 'b' = 127)))
    */

)
ENGINE = MergeTree ORDER BY (id);

INSERT INTO canon.table VALUES (1,    /* All data types */    -128,    -32768,    -2147483648,    -9223372036854775808,    /* -170141183460469231731687303715884105728, -- Error: unhandled type Int128 */    /* -57896044618658097711785492504343953926634992332820282019728792003956564819968, -- Error: unhandled type Int256 */    0,      0,        0,             0,                       /* 0, -- Error: unhandled type UInt128 */                                          /* 0, -- Error: unhandled type UInt256 */                                                                                 1.175494351E-38,    2.2250738585072014E-308,    1.23,    1.23,    123456789012345678901234567890.23,    1234567890123456789012345678901234567890123456789012345678901234567890.23,    /* false, -- Error: unhandled type Bool */    'clickhouse',    'clickhouse',    'clickhouse',    '61f0c404-5cb3-11e7-907b-a6006ad3dba0',    '1970-01-01',    /* '1900-01-01', -- Error: unhandled type Date32 */    '1970-01-01 00:00:00',    '1900-01-01 00:00:00',    '1900-01-01 00:00:00',        '1900-01-01 00:00:00',           'a',    'a',    /* '{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}', -- Error: unhandled type 'a Int8', unhandled type 'a String' */    /* Nullable */    null,    null,    null,    null,    /* null, -- Error: unhandled type Int256 */    /* null, -- Error: unhandled type Int256 */    null,    null,    null,    null,    /* null, -- Error: unhandled type UInt256 */    /* null, -- Error: unhandled type UInt256 */    null,    null,    null,    null,    /* null, -- Error: reflect: call of reflect.Value.Interface on zero Value */    /* null, -- Error: Nullable(T): precision of Decimal exceeds max bound */    /* null, -- Error: unhandled type Bool */    null,            null,            null,            null,                                      null,            /* null, -- Error: unhandled type Date32 */            null,                     null,                     null,                     null,                     null,    null,    /* Arrays */    [null, 1],    [null, 1],    [null, 1],    [null, 1],    /* [null, 1], -- Error: unhandled type Int128 */    /* [null, 1], -- Error: unhandled type Int256 */    [null, 1],    [null, 1],    [null, 1],    [null, 1],    /* [null, 1], -- Error: unhandled type UInt128 */    /* [null, 1], -- Error: unhandled type UInt256 */    [null, 1],    [null, 1],    [null, 1],    [null, 1],    /* [null, 1], -- Error: unsupported Array type */    /* [null, 1], -- Error: Array(T): Nullable(T): precision of Decimal exceeds max bound */    /* [null, false], -- Error: unhandled type Bool */    [null, '1'],    [null, '1'],    [null, '1'],    [null, '61f0c404-5cb3-11e7-907b-a6006ad3dba0'],    /* Arrays with dates/datetimes reads incorrectly, with strange timezones (+3h/+2.5h)    [null, '1970-01-01'],    [null, '1900-01-01'], -- Error: unhandled type Date32    [null, '1970-01-01 00:00:00'],    [null, '1900-01-01 00:00:00'],    [null, '1900-01-01 00:00:00'],    [null, '1900-01-01 00:00:00'], */    [null, 'a'],    [null, 'a']    /* Maps */    /*    Map types are not supported, error: unhandled type Map(<type>,<type>)    map('1', null, '2' ,'clickhouse'),    map(1, null, 2, 222),    map('1', null, '2', 'clickhouse'),    map('61f0c404-5cb3-11e7-907b-a6006ad3dba0', null, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'),    map('1970-01-01', null, '1970-01-01', '1970-01-01'),    map('1970-01-01 00:00:00', null, '1970-01-01 00:00:00', '1970-01-01 00:00:00'),    map('1900-01-01', null, '1900-01-01', '1900-01-01'),    map('a', null, 'a', 'b')    */);
INSERT INTO canon.table VALUES (2,    /* All data types */    127,      32767,    2147483647,     9223372036854775807,     /* 170141183460469231731687303715884105727, -- Error: unhandled type Int128 */     /* 57896044618658097711785492504343953926634992332820282019728792003956564819967, -- Error: unhandled type Int256 */     255,    65535,    4294967295,    18446744073709551615,    /* 340282366920938463463374607431768211455, -- Error: unhandled type UInt128 */    /* 115792089237316195423570985008687907853269984665640564039457584007913129639935, -- Error: unhandled type UInt256 */    3.02823466E+38,     1.7976931348623158E+308,    3.21,    3.21,    3.21,                                 3.21,                                                                         /* true, -- Error: unhandled type Bool */     'clickhouse',    'clickhouse',    'clickhouse',    '61f0c404-5cb3-11e7-907b-a6006ad3dba0',    '2149-06-06',    /* '2299-12-31', -- Error: unhandled type Date32 */    '2106-02-07 06:28:15',    '2299-12-31 23:59:59',    '2299-12-31 23:59:59.999',    '2299-12-31 23:59:59.999999',    'b',    'b',    /* '{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}', -- Error: unhandled type 'a Int8', unhandled type 'a String' */    /* Nullable */    1,       1,       1,       1,       /* 1, -- Error: unhandled type Int128 */       /* 1, -- Error: unhandled type Int256 */       1,       1,       1,       1,       /* 1, -- Error: unhandled type UInt128 */       /* 1, -- Error: unhandled type UInt256 */       1,       1,       1,       1,       /* 1, -- Error: reflect: call of reflect.Value.Interface on zero Value */       /* 1, -- Error: Nullable(T): precision of Decimal exceeds max bound */       /* true, -- Error: unhandled type Bool */    'clickhouse',    'clickhouse',    'clickhouse',    '61f0c404-5cb3-11e7-907b-a6006ad3dba0',    '2022-08-29',    /* '2022-08-29', -- Error: unhandled type Date32 */    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    'a',    'a',      /* Arrays */    [0, 1],       [0, 1],       [0, 1],       [0, 1],       /* [0, 1], -- Error: unhandled type Int128 */       /* [0, 1], -- Error: unhandled type Int256 */       [0, 1],       [0, 1],       [0, 1],       [0, 1],       /* [0, 1], -- Error: unhandled type UInt128 */       /* [0, 1], -- Error: unhandled type UInt256 */       [0, 1],       [0, 1],       [0, 1],       [0, 1],       /* [0, 1], -- Error: unsupported Array type */       /* [0, 1], -- Error: Array(T): Nullable(T): precision of Decimal exceeds max bound */       /* [true, false], -- Error: unhandled type Bool */    ['0', '1'],     ['0', '1'],     ['0', '1'],     ['61f0c404-5cb3-11e7-907b-a6006ad3dba0'],          /* Arrays with dates/datetimes reads incorrectly, with strange timezones (+3h/+2.5h)    ['1970-01-01'],          ['1900-01-01'], -- Error: unhandled type Date32          ['1970-01-01 00:00:00'],          ['1900-01-01 00:00:00'],          ['1900-01-01 00:00:00'],          ['1900-01-01 00:00:00'],       */    ['b', 'a'],     ['b', 'a']     /* Maps */    /*    Map types are not supported, error: unhandled type Map(<type>,<type>)    map('2' ,'clickhouse'),               map(2, 222),             map('2', 'clickhouse'),               map('61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'),                                                  map('1970-01-01', '1970-01-01'),                        map('1970-01-01 00:00:00', '1970-01-01 00:00:00'),                                 map('1900-01-01', '1900-01-01'),                        map('a', 'b')               */);
INSERT INTO canon.table VALUES (3,    /* All data types */    2,        2,        2,              2,                       /* 2, -- Error: unhandled type Int128 */                                           /* 2, -- Error: unhandled type Int256 */                                                                                 2,      2,        2,             2,                       /* 2, -- Error: unhandled type UInt128 */                                          /* 2, -- Error: unhandled type UInt256 */                                                                                 2.2,                2.2,                        2.10,    2.10,    2.10,                                 2.10,                                                                         /* true, -- Error: unhandled type Bool */     'clickhouse',    'clickhouse',    'clickhouse',    '61f0c404-5cb3-11e7-907b-a6006ad3dba0',    '2022-08-29',    /* '2022-08-29', -- Error: unhandled type Date32 */    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',        '2022-08-29 19:04:00',           'b',    'b',    /* '{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}', -- Error: unhandled type 'a Int8', unhandled type 'a String' */    /* Nullable */    1,       1,       1,       1,       /* 1, -- Error: unhandled type Int128 */       /* 1, -- Error: unhandled type Int256 */       1,       1,       1,       1,       /* 1, -- Error: unhandled type UInt128 */       /* 1, -- Error: unhandled type UInt256 */       1,       1,       1,       1,       /* 1, -- Error: reflect: call of reflect.Value.Interface on zero Value */       /* 1, -- Error: Nullable(T): precision of Decimal exceeds max bound */       /* true, -- Error: unhandled type Bool */    'clickhouse',    'clickhouse',    'clickhouse',    '61f0c404-5cb3-11e7-907b-a6006ad3dba0',    '2022-08-29',    /* '2022-08-29', -- Error: unhandled type Date32 */    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    '2022-08-29 19:04:00',    'a',    'a',      /* Arrays */    [0, 1],       [0, 1],       [0, 1],       [0, 1],       /* [0, 1], -- Error: unhandled type Int128 */       /* [0, 1], -- Error: unhandled type Int256 */       [0, 1],       [0, 1],       [0, 1],       [0, 1],       /* [0, 1], -- Error: unhandled type UInt128 */       /* [0, 1], -- Error: unhandled type UInt256 */       [0, 1],       [0, 1],       [0, 1],       [0, 1],       /* [0, 1], -- Error: unsupported Array type */       /* [0, 1], -- Error: Array(T): Nullable(T): precision of Decimal exceeds max bound */       /* [true, false], -- Error: unhandled type Bool */    ['0', '1'],     ['0', '1'],     ['0', '1'],     ['61f0c404-5cb3-11e7-907b-a6006ad3dba0'],          /* Arrays with dates/datetimes reads incorrectly, with strange timezones (+3h/+2.5h)    ['2022-08-29'],          ['2022-08-29'], -- Error: unhandled type Date32          ['2022-08-29 19:04:00'],          ['2022-08-29 19:04:00'],          ['2022-08-29 19:04:00'],          ['2022-08-29 19:04:00'],       */    ['b', 'a'],     ['b', 'a']     /* Maps */    /*    Map types are not supported, error: unhandled type Map(<type>,<type>)    map('2' ,'clickhouse'),               map(2, 222),             map('2', 'clickhouse'),               map('61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'),                                                  map('2022-08-29', '2022-08-29'),                        map('2022-08-29 19:04:00', '2022-08-29 19:04:00'),                                 map('2022-08-29', '2022-08-29'),                        map('a', 'b')               */);
