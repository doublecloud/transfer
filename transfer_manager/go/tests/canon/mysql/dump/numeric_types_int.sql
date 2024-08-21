create table if not exists numeric_types_int
(
    __primary_key INT PRIMARY KEY,

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
    t_bigint_u BIGINT UNSIGNED
);


INSERT INTO numeric_types_int (__primary_key, t_tinyint,t_tinyint_default0,t_tinyint_u,t_tinyint1,t_tinyint1u,t_smallint,t_smallint5,t_smallint_u,t_mediumint,t_mediumint5,t_mediumint_u,t_int,       t_integer,  t_integer5,t_int_u,t_bigint,             t_bigint5,t_bigint_u) VALUES (12000,
                                              -128,     -128,              0,          -1,        0,          -32768,    -32768,     0,           -8388608,   -99999,      0,            -2147483648, -2147483648,-99999,    0,      -9223372036854775807, -99999,   0);
INSERT INTO numeric_types_int (__primary_key, t_tinyint,t_tinyint_default0,t_tinyint_u,t_tinyint1,t_tinyint1u,t_smallint,t_smallint5,t_smallint_u,t_mediumint,t_mediumint5,t_mediumint_u,t_int,       t_integer,  t_integer5,t_int_u,t_bigint,             t_bigint5,t_bigint_u) VALUES (12001,
                                              127,      127,               256,        1,         1,          32767,     32767,      65535,       8388607,    99999,       16777215,     2147483647,  2147483647, 99999,     0,      -9223372036854775807, -99999,   18446744073709551615);


-- null case
INSERT INTO numeric_types_int (__primary_key, t_tinyint_default0) VALUES (801640048, NULL);
