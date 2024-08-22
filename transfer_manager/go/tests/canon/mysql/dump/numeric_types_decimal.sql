create table if not exists numeric_types_decimal
(
    __primary_key INT PRIMARY KEY,

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


INSERT INTO numeric_types_decimal (__primary_key,t_decimal, t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,     t_fixed,   t_numeric, t_decimal_65) VALUES (13000,
                                                 1234567890,12345,      123.45,       1234567890, 1234567890,1234567890,1234567890,12345678901234567890123456789012345678901234567890123456789012345);
INSERT INTO numeric_types_decimal (__primary_key,t_decimal,  t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,      t_fixed,    t_numeric,  t_decimal_65) VALUES (13001,
                                                 -1234567890,-12345,     -123.45,      0,          -1234567890,-1234567890,-1234567890,-12345678901234567890123456789012345678901234567890123456789012345);
INSERT INTO numeric_types_decimal (__primary_key,t_decimal,t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,t_fixed,t_numeric,  t_decimal_65) VALUES (13002,
                                                 0,        0,          0,            0,          0,    0,      0,          0);
INSERT INTO numeric_types_decimal (__primary_key,t_decimal, t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,     t_fixed,   t_numeric, t_decimal_65) VALUES (13003,
                                                 9999999999,99999,      999.99,       99999999999, 999999999,9999999999,9999999999,99999999999999999999999999999999999999999999999999999999999999999);
INSERT INTO numeric_types_decimal (__primary_key,t_decimal,  t_decimal_5,t_decimal_5_2,t_decimal_u,t_dec,      t_fixed,    t_numeric,  t_decimal_65) VALUES (13004,
                                                 -9999999999,-99999,     -999.99,      0,          -9999999999,-9999999999,-9999999999,-99999999999999999999999999999999999999999999999999999999999999999);


-- null case
INSERT INTO numeric_types_decimal (__primary_key) VALUES (801640048);
