CREATE TABLE public.basic_types
(
    bl  boolean,
    b   bit(1),
    b8  bit(8),
    vb  varbit(8),

    si   smallint,
    ss   smallserial,
    int  integer primary key ,
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
    DATERANGE_ DATERANGE
    -- ENUM
);

INSERT INTO public.basic_types VALUES (
                                          true,
                                          b'1',
                                          b'10101111',
                                          b'10101110',

                                          -32768,
                                          1,
                                          -8388605,
                                          0,
                                          1,
                                          3372036854775807,
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
                                          daterange('2000-01-10'::date, '2000-01-20'::date, '[]')
                                      );
