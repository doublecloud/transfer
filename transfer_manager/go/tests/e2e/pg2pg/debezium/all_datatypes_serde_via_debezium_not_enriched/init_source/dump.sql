CREATE EXTENSION hstore;
CREATE EXTENSION ltree;
CREATE EXTENSION citext;

CREATE TABLE public.basic_types
(
    bl  boolean,
    b   bit(1),
    b8  bit(8),
    vb  varbit(8),

    si   smallint,
--     ss   smallserial,
    int  integer,
--     aid  serial,
    id   bigint,
--     bid  bigserial,
    oid_ oid,

    real_ real,
    d   double precision,

    c   char,
    str varchar(256),

    CHARACTER_ CHARACTER(4),
    CHARACTER_VARYING_ CHARACTER VARYING(5),
    TIMESTAMPTZ_ TIMESTAMPTZ, -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a PostgreSQL extension
    tst TIMESTAMP WITH TIME ZONE,
    TIMETZ_ TIMETZ,
    TIME_WITH_TIME_ZONE_ TIME WITH TIME ZONE,
    iv  interval,
    ba  bytea,

    j   json,
    jb  jsonb,
    x   xml,

    uid uuid,
    pt  point,
    it  inet,
    INT4RANGE_ INT4RANGE,
    INT8RANGE_ INT8RANGE,
    NUMRANGE_ NUMRANGE,
    TSRANGE_ TSRANGE,
    TSTZRANGE_ TSTZRANGE,
    DATERANGE_ DATERANGE,
    -- ENUM

    -- add, from our /Users/timmyb32r/arc/arcadia/transfer_manager/go/tests/e2e/pg2pg/debezium/replication/dump/type_check.sql:
    f   float,
    i   int PRIMARY KEY,
    t   text,

    -- ----------------------------------------------------------------------------------------------------------------

    DATE_ DATE,
    TIME_ TIME,
    TIME1 TIME(1), -- precision: This is a fractional digits number placed in the seconds’ field. This can be up to six digits. HH:MM:SS.pppppp
    TIME6 TIME(6),

    TIMETZ__ TIME WITH TIME ZONE,
    TIMETZ1 TIME(1) WITH TIME ZONE,
    TIMETZ6 TIME(6) WITH TIME ZONE,

    TIMESTAMP1 TIMESTAMP(1),
    TIMESTAMP6 TIMESTAMP(6),
    TIMESTAMP TIMESTAMP,

    --NUMERIC(precision) # selects a scale of 0
    --NUMERIC(precision, scale)
    -- 'numeric' type - it's bignum
    -- precision - digits in the whole number, that is, the number of digits to both sides of the decimal point
    -- scale     - count of decimal digits in the fractional part, to the right of the decimal point
    --
    -- example: So the number 23.5141 has a precision of 6 and a scale of 4. Integers can be considered to have a scale of zero
    -- In addition to ordinary numeric values, the numeric type has several special values:
    --     Infinity
    --     -Infinity
    --     NaN
    NUMERIC_ NUMERIC,
    NUMERIC_5 NUMERIC(5),
    NUMERIC_5_2 NUMERIC(5,2),

    --DECIMAL
    -- The types decimal and numeric are equivalent
    DECIMAL_ DECIMAL,
    DECIMAL_5 DECIMAL(5),
    DECIMAL_5_2 DECIMAL(5,2),

    --MONEY
    -- The money type stores a currency amount with a fixed fractional precision
    --     [local] =# CREATE TABLE money_example (cash money);
    --     [local] =# INSERT INTO money_example VALUES ('$99.99');
    --     [local] =# INSERT INTO money_example VALUES (99.99);
    --     [local] =# INSERT INTO money_example VALUES (99.98996998);
    MONEY_ MONEY,

    HSTORE_ HSTORE,
    INET_ INET,
    CIDR_ CIDR,
    MACADDR_ MACADDR,
    -- MACADDR8 not supported by postgresql 9.6 (which is in our recipes)
    -- LTREE - should be in special table, I suppose
    CITEXT_ CITEXT
);

INSERT INTO public.basic_types VALUES (
    true,
    b'1',
    b'10101111',
    b'10101110',

    -32768,
--     1,
    -8388605,
--     0,
    1,
--     3372036854775807,
    2,

    1.45e-10,
    3.14e-100,

    '1',
    'varchar_example',

    'abcd',
    'varc',
    '2004-10-19 10:23:54+02',
    '2004-10-19 11:23:54+02',
    '00:51:02.746572-08',
    '00:51:02.746572-08',
    interval '1 day 01:00:00',
    decode('CAFEBABE', 'hex'),

    '{"k1": "v1"}',
    '{"k2": "v2"}',
    '<foo>bar</foo>',

    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    point(23.4, -44.5),
    '192.168.100.128/25',
    '[3,7)'::int4range,
    '[3,7)'::int8range,
    numrange(1.9,1.91),
    '[2010-01-02 10:00, 2010-01-02 11:00)',
    '[2010-01-01 01:00:00 -05, 2010-01-01 02:00:00 -08)'::tstzrange,
    daterange('2000-01-10'::date, '2000-01-20'::date, '[]'),

    1.45e-10,
    1,
    'text_example',

    -- ----------------------------------------------------------------------------------------------------------------

    --     DATE_ DATE,
    'January 8, 1999',

    --     TIME_ TIME,
    --     TIME1 TIME(1), -- precision: This is a fractional digits number placed in the seconds’ field. This can be up to six digits. HH:MM:SS.pppppp
    --     TIME6 TIME(6),
    '04:05:06',
    '04:05:06.1',
    '04:05:06.123456',

    --     TIMETZ__ TIME WITH TIME ZONE,
    --     TIMETZ1 TIME(1) WITH TIME ZONE,
    --     TIMETZ6 TIME(6) WITH TIME ZONE,
    '2020-05-26 13:30:25-04',
    '2020-05-26 13:30:25.5-04',
    '2020-05-26 13:30:25.575401-04',

    --     TIMESTAMP1 TIMESTAMP(1),
    --     TIMESTAMP6 TIMESTAMP(6),
    --     TIMESTAMP TIMESTAMP,
    '2004-10-19 10:23:54.9',
    '2004-10-19 10:23:54.987654',
    '2004-10-19 10:23:54',

    --
    --     NUMERIC_ NUMERIC,
    --     NUMERIC_5 NUMERIC(5),
    --     NUMERIC_5_2 NUMERIC(5,2),
    1267650600228229401496703205376,
    12345,
    123.67,

    --     DECIMAL_ DECIMAL,
    --     DECIMAL_5 DECIMAL(5),
    --     DECIMAL_5_2 DECIMAL(5,2),
    123456,
    12345,
    123.67,

    --     MONEY_ MONEY,
    99.98,

    --     HSTORE_ HSTORE,
    'a=>1,b=>2',

    --     INET_ INET,
    '192.168.1.5',

    --     CIDR_ CIDR,
    '10.1/16',

    --     MACADDR_ MACADDR,
    '08:00:2b:01:02:03',

    --     CITEXT_ CITEXT
    'Tom'
);
