CREATE EXTENSION hstore;
CREATE EXTENSION ltree;
CREATE EXTENSION citext;

CREATE TABLE public.basic_types
(
    i   int PRIMARY KEY,

    -- ----------------------------------------------------------------------------------------------------------------

    ARR_bl  boolean[],
    -- ARR_b   bit(1)[],
    -- ARR_b8  bit(8)[],
    -- ARR_vb  varbit(8)[],

    ARR_si   smallint[],
    -- ARR_ss   smallserial[],
    ARR_int  integer[],
    -- ARR_aid  serial[],
    ARR_id   bigint[],
    -- ARR_bid  bigserial[],
    ARR_oid_ oid[],

    ARR_real_ real[],
    ARR_d   double precision[],

    ARR_c   char[],
    ARR_str varchar(256)[],

    ARR_CHARACTER_ CHARACTER(4)[],
    ARR_CHARACTER_VARYING_ CHARACTER VARYING(5)[],
    ARR_TIMESTAMPTZ_ TIMESTAMPTZ[], -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a PostgreSQL extension
    ARR_tst TIMESTAMP WITH TIME ZONE[],
    ARR_TIMETZ_ TIMETZ[],
    ARR_TIME_WITH_TIME_ZONE_ TIME WITH TIME ZONE[],
    -- ARR_iv  interval[],
    -- ARR_ba  bytea[],

    -- ARR_j   json[],
    -- ARR_jb  jsonb[],
    -- ARR_x   xml[],

    ARR_uid uuid[],
    -- ARR_pt  point[],
    ARR_it  inet[],
    -- ARR_INT4RANGE_ INT4RANGE[],
    -- ARR_INT8RANGE_ INT8RANGE[],
    -- ARR_NUMRANGE_ NUMRANGE[],
    -- ARR_TSRANGE_ TSRANGE[],
    -- ARR_TSTZRANGE_ TSTZRANGE[],
    -- ARR_DATERANGE_ DATERANGE[],
    -- ENUM

    -- add, from our /Users/timmyb32r/arc/arcadia/transfer_manager/go/tests/e2e/pg2pg/replication/dump/type_check.sql:
    ARR_f   float[],
    ARR_i   int[],
    ARR_t   text[],

    -- ----------------------------------------------------------------------------------------------------------------

    ARR_DATE_ DATE[],
    ARR_TIME_ TIME[],
    ARR_TIME1 TIME(1)[], -- precision: This is a fractional digits number placed in the seconds’ field. This can be up to six digits. HH:MM:SS.pppppp
    ARR_TIME6 TIME(6)[],

    ARR_TIMETZ__ TIME WITH TIME ZONE[],
    ARR_TIMETZ1 TIME(1) WITH TIME ZONE[],
    ARR_TIMETZ6 TIME(6) WITH TIME ZONE[],

    ARR_TIMESTAMP1 TIMESTAMP(1)[],
    ARR_TIMESTAMP6 TIMESTAMP(6)[],
    ARR_TIMESTAMP TIMESTAMP[],

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
    ARR_NUMERIC_ NUMERIC[],
    ARR_NUMERIC_5 NUMERIC(5)[],
    ARR_NUMERIC_5_2 NUMERIC(5,2)[],

    --DECIMAL
    -- The types decimal and numeric are equivalent
    ARR_DECIMAL_ DECIMAL[],
    ARR_DECIMAL_5 DECIMAL(5)[],
    ARR_DECIMAL_5_2 DECIMAL(5,2)[]

--     ARR_HSTORE_ HSTORE[],
--     ARR_INET_ INET[],
--     ARR_CIDR_ CIDR[],
--     ARR_MACADDR_ MACADDR[],
--     -- MACADDR8 not supported by postgresql 9.6 (which is in our recipes)
--     -- LTREE - should be in special table, I suppose
--     ARR_CITEXT_ CITEXT[]
);
