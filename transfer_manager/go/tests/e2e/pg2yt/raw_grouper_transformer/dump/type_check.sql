-- needs to be sure there is db1
create table __test (
                        id  bigint not null,
                        aid serial,

    -- numeric
                        f    float,
                        d    double precision,
                        de   decimal(10,2),

                        i    int,
                        bi   bigint,
                        biu  bigint,
                        b    bit(8),

    -- date time
                        da   date,
                        ts   timestamp without time zone default (now()),
                        dt   timestamp with time zone default (now()),

    -- strings
                        c    char,
                        str  varchar(256),
                        t    text,
--     bb   blob,

    -- binary
--     bin  binary(10),
--     vbin varbinary(100),

    -- other
--      e    enum ("e1", "e2"),
--      se   set('a', 'b', 'c'),
                        _rest json,
                        primary key (aid, str, id) -- test multi pk and reverse order keys
);

insert into __test values (
                              1,
                              0,
                              1.45e-10,
                              3.14e-100,
                              2.5,
                              -8388605,
                              2147483642,
                              9223372036854775804,

                              b'10101111',

                              '2005-03-04',
                              now(),
                              now(),


                              '1',
                              'hello, friend of mine',
                              'okay, now bye-bye',

--                             "e1",           -- e
--                             'a',            -- se
                            '{"yandex is the best place to work at": ["wish i", "would stay", 4.15, {"here after":"the  "}, ["i", ["n", ["t", "e r n s h i"], "p"]]]}'
                          )
    ,
                          (
                              2,
                              1,
                              1.34e-10,
                              null,
                              null,
                              -1294129412,
                              112412412421941041,
                              129491244912401240,

                              b'10000001',

                              '1999-03-04',
                              now(),
                              null,
--     now(),
--     '1971', -- year

                              '2',
                              'another hello',
                              'okay, another bye',

--                             "e2",           -- e
--                             'b',            -- se
                            '{"simpler": ["than", 13e-10, {"it": {"could": "be"}}]}'
                          )
    ,
                          (
                              3,
                              4,
                              5.34e-10,
                              null,
                              123,
                              294129412,
                              -784124124219410491,
                              129491098649360240,

                              b'10000010',

                              '1999-03-05',
                              null,
                              now(),

                              'c',
                              'another another hello',
                              'okay, another another bye',

--                             "e1",                   -- e
--                             'c',                    -- se
                            '{"simpler": ["than", 13e-10, {"it": {"could": ["be", "no", "ideas    ", "   again"], "sorry": null}}]}'
                          )
;

insert into __test (str, id) values ('hello', 0),
                                    ('aaa', 214),
                                    ('vvvv', 124124),
                                    ('agpnaogapoajfqt-oqoo ginsdvnaojfspbnoaj apngpowo qeonwpbwpen', 1234),
                                    ('aagiangsfnaofasoasvboas', 12345);

insert into __test (str, id, da) values ('nvaapsijfapfn', 201, now()),
                                        ('Day the creator of this code was born', 202, '1999-09-16'),
                                        ('Coronavirus made me leave', 322, '2020-06-03'),
                                        ('But Ill be back, this is public promise', 422, now()),
                                        ('Remember me, my name is hazzus', 333, now());

