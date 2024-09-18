create table if not exists numeric_types_float
(
    __primary_key INT PRIMARY KEY,

    -- float
    -- no limits in doc: https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html

    t_real      REAL,
    t_real_10_2 REAL(10, 2),

    t_float            FLOAT,
    t_float_53         FLOAT(53), -- doc: A precision from 0 to 23 results in a 4-byte single-precision FLOAT column. A precision from 24 to 53 results in an 8-byte double-precision DOUBLE column.

    t_double           DOUBLE,
    t_double_precision DOUBLE PRECISION
);


INSERT INTO numeric_types_float (__primary_key, t_real,  t_real_10_2, t_float,  t_float_53,                                             t_double,  t_double_precision) VALUES (14000,
                                                1.45e-45,12345678.90, 3.14e-324,-12345678901234567890123456789012345678901234567890123, 3.14e-324, 3.14e-324);
INSERT INTO numeric_types_float (__primary_key, t_real,          t_real_10_2, t_float,                  t_float_53,                                            t_double,                 t_double_precision) VALUES (14001,
                                                1.175494351E-38, 99999999.99, -1.7976931348623157E+308, 99999999999999999999999999999999999999999999999999999, -1.7976931348623157E+308, -1.7976931348623157E+308);
INSERT INTO numeric_types_float (__primary_key, t_real,                 t_real_10_2,            t_float,                t_float_53,             t_double,               t_double_precision) VALUES (14002,
                                                123456.123456789012345, 123456.123456789012345, 123456.123456789012345, 123456.123456789012345, 123456.123456789012345, 123456.123456789012345);
INSERT INTO numeric_types_float (__primary_key, t_real,                  t_real_10_2, t_float, t_float_53, t_double,                t_double_precision) VALUES (14003,
                                                1.7976931348623157E+308, 00000000.00, 3.4E+38, 3.4E+38,    1.7976931348623157E+308, 1.7976931348623157E+308);
INSERT INTO numeric_types_float (__primary_key, t_float) VALUES (13005, -340282346638528859811704183484516925440.0000000000000000);
INSERT INTO numeric_types_float (__primary_key, t_float) VALUES (13006, 340282346638528859811704183484516925440.0000000000000000);
INSERT INTO numeric_types_float (__primary_key, t_double) VALUES (13007, -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.0000000000000000);
INSERT INTO numeric_types_float (__primary_key, t_double) VALUES (13008, 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.0000000000000000);


-- null case
INSERT INTO numeric_types_float (__primary_key) VALUES (801640048);
