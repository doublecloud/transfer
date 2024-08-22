create table if not exists numeric_types
(
    __primary_key INT PRIMARY KEY,

    -- bool

    t_boolean BOOLEAN,
    t_bool    BOOL,

    -- bit
    -- https://dev.mysql.com/doc/refman/8.0/en/bit-type.html
    -- A type of BIT(M) enables storage of M-bit values. M can range from 1 to 64.

    t_bit   BIT,
    t_bit1  BIT(1),
    t_bit16 BIT(16),
    t_bit64 BIT(64),

    -- int
    -- limits: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html

    t_tinyint          TINYINT,
    t_tinyint_default0 TINYINT DEFAULT 0,
    t_tinyint_u        TINYINT UNSIGNED,

    t_tinyint1  TINYINT(1),
    t_tinyint1u TINYINT(1) UNSIGNED,

    t_smallint   SMALLINT,
    t_smallint5  SMALLINT(5),
    t_smallint_u SMALLINT UNSIGNED,

    t_mediumint   MEDIUMINT,
    t_mediumint5  MEDIUMINT(5),
    t_mediumint_u MEDIUMINT UNSIGNED,

    t_int      INT,
    t_integer  INTEGER,
    t_integer5 INTEGER(5), -- doc: For example, INT(4) specifies an INT with a display width of four digits
    t_int_u    INT UNSIGNED,

    t_bigint   BIGINT,
    t_bigint5  BIGINT(5),
    t_bigint_u BIGINT UNSIGNED,

    -- float
    -- no limits in doc: https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html

    t_real      REAL,
    t_real_10_2 REAL(10, 2),

    t_float            FLOAT,
    t_float_53         FLOAT(53), -- doc: A precision from 0 to 23 results in a 4-byte single-precision FLOAT column. A precision from 24 to 53 results in an 8-byte double-precision DOUBLE column.

    t_double           DOUBLE,
    t_double_precision DOUBLE PRECISION,

    -- numeric
    -- https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html

    t_decimal     DECIMAL,
    t_decimal_5   DECIMAL(5),
    t_decimal_5_2 DECIMAL(5,2),
    t_decimal_u   DECIMAL UNSIGNED,

    t_dec DEC,
    t_fixed FIXED,
    t_numeric NUMERIC,

    t_decimal_65 DECIMAL(65)
);

INSERT INTO numeric_types (__primary_key, t_boolean, t_bool) VALUES (10000, false, false);
INSERT INTO numeric_types (__primary_key, t_boolean, t_bool) VALUES (10001, true, true);


INSERT INTO numeric_types (__primary_key, t_bit, t_bit1, t_bit16, t_bit64) VALUES (11000, b'0', b'0', b'0000000000000000', b'0000000000000000000000000000000000000000000000000000000000000000');
INSERT INTO numeric_types (__primary_key, t_bit, t_bit1, t_bit16, t_bit64) VALUES (11001, b'1', b'1', b'1111111111111111', b'1111111111111111111111111111111111111111111111111111111111111111');


INSERT INTO numeric_types (__primary_key, t_tinyint,t_tinyint_default0,t_tinyint_u,t_tinyint1,t_tinyint1u,t_smallint,t_smallint5,t_smallint_u,t_mediumint,t_mediumint5,t_mediumint_u,t_int,       t_integer,  t_integer5,t_int_u,t_bigint,             t_bigint5,t_bigint_u) VALUES (12000,
                                          -128,     -128,              0,          -1,        0,          -32768,    -32768,     0,           -8388608,   -99999,      0,            -2147483648, -2147483648,-99999,    0,      -9223372036854775807, -99999,   0);
INSERT INTO numeric_types (__primary_key, t_tinyint,t_tinyint_default0,t_tinyint_u,t_tinyint1,t_tinyint1u,t_smallint,t_smallint5,t_smallint_u,t_mediumint,t_mediumint5,t_mediumint_u,t_int,       t_integer,  t_integer5,t_int_u,t_bigint,             t_bigint5,t_bigint_u) VALUES (12001,
                                          127,      127,               256,        1,         1,          32767,     32767,      65535,       8388607,    99999,       16777215,     2147483647,  2147483647, 99999,     0,      -9223372036854775807, -99999,   18446744073709551615);


INSERT INTO numeric_types (__primary_key, t_real,  t_real_10_2, t_float,  t_float_53,                                             t_double,  t_double_precision) VALUES (14000,
                                          1.45e-45,12345678.90, 3.14e-324,-12345678901234567890123456789012345678901234567890123, 3.14e-324, 3.14e-324);
INSERT INTO numeric_types (__primary_key, t_real,          t_real_10_2, t_float,                  t_float_53,                                            t_double,                 t_double_precision) VALUES (14001,
                                          1.175494351E-38, 99999999.99, -1.7976931348623157E+308, 99999999999999999999999999999999999999999999999999999, -1.7976931348623157E+308, -1.7976931348623157E+308);
INSERT INTO numeric_types (__primary_key, t_real,                 t_real_10_2,            t_float,                t_float_53,             t_double,               t_double_precision) VALUES (14002,
                                          123456.123456789012345, 123456.123456789012345, 123456.123456789012345, 123456.123456789012345, 123456.123456789012345, 123456.123456789012345);
INSERT INTO numeric_types (__primary_key, t_real,                  t_real_10_2, t_float, t_float_53, t_double,                t_double_precision) VALUES (14003,
                                          1.7976931348623157E+308, 00000000.00, 3.4E+38, 3.4E+38,    1.7976931348623157E+308, 1.7976931348623157E+308);
INSERT INTO numeric_types (__primary_key, t_float) VALUES (13005, -340282346638528859811704183484516925440.0000000000000000);
INSERT INTO numeric_types (__primary_key, t_float) VALUES (13006, 340282346638528859811704183484516925440.0000000000000000);
INSERT INTO numeric_types (__primary_key, t_double) VALUES (13007, -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.0000000000000000);
INSERT INTO numeric_types (__primary_key, t_double) VALUES (13008, 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.0000000000000000);


INSERT INTO numeric_types (__primary_key,t_decimal, t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,     t_fixed,   t_numeric, t_decimal_65) VALUES (13000,
                                         1234567890,12345,      123.45,       1234567890, 1234567890,1234567890,1234567890,12345678901234567890123456789012345678901234567890123456789012345);
INSERT INTO numeric_types (__primary_key,t_decimal,  t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,      t_fixed,    t_numeric,  t_decimal_65) VALUES (13001,
                                         -1234567890,-12345,     -123.45,      0,          -1234567890,-1234567890,-1234567890,-12345678901234567890123456789012345678901234567890123456789012345);
INSERT INTO numeric_types (__primary_key,t_decimal,t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,t_fixed,t_numeric,  t_decimal_65) VALUES (13002,
                                         0,        0,          0,            0,          0,    0,      0,          0);
INSERT INTO numeric_types (__primary_key,t_decimal, t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,     t_fixed,   t_numeric, t_decimal_65) VALUES (13003,
                                         9999999999,99999,      999.99,       99999999999, 999999999,9999999999,9999999999,99999999999999999999999999999999999999999999999999999999999999999);
INSERT INTO numeric_types (__primary_key,t_decimal,  t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,      t_fixed,    t_numeric,  t_decimal_65) VALUES (13004,
                                         -9999999999,-99999,     -999.99,      0,          -9999999999,-9999999999,-9999999999,-99999999999999999999999999999999999999999999999999999999999999999);


-- null case
INSERT INTO numeric_types (__primary_key, t_tinyint_default0) VALUES (801640048, NULL);
