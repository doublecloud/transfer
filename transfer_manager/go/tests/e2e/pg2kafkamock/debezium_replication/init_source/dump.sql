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
    ss   smallserial,
    int  integer,
    aid  serial,
    id   bigint,
    bid  bigserial,
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

    -- add, from our /Users/timmyb32r/arc/arcadia/transfer_manager/go/tests/e2e/pg2pg/replication/dump/type_check.sql:
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
    -- MONEY_ MONEY,

    HSTORE_ HSTORE,
    INET_ INET,
    CIDR_ CIDR,
    MACADDR_ MACADDR,
    -- MACADDR8 not supported by postgresql 9.6 (which is in our recipes)
    -- LTREE - should be in special table, I suppose
    CITEXT_ CITEXT
);
