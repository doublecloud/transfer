create table __test (
    id  bigint not null,
    aid serial,
    f    float,
    d    double precision,
    de   decimal(10,2),
    i    int,
    bi   bigint,
    biu  bigint,
    b    bit(8),
    da   date,
    ts   timestamp,
    dt   timestamp,
    c    char,
    str  varchar(256),
    t    text,
    primary key (aid, id) -- test multi pk and reverse order keys
);

insert into __test values (
    1, -- id
    0, -- aid
    1.45e-10, -- f
    3.14e-100, -- d
    2.5, -- de
    -8388605, -- i
    2147483642, -- bi
    9223372036854775804, --biu
    b'10101111', -- b
    '2005-03-04', -- da
   now(), -- ts
   now(), -- dt
    '1', -- c
    'hello, friend of mine', -- str
    'okay, now bye-bye' -- t
)
,
(
    2, -- id
    1, -- aid
    1.34e-10, -- f
    null, -- d
    null, -- de
    -1294129412, -- i
    112412412421941041, -- bi
    129491244912401240, --biu
    b'10000001', -- b
    '1999-03-04', -- da
    now(), -- ts
    null, -- dt
    '2', -- c
    'another hello', -- str
    'okay, another bye' -- t
)
,
(
    3, -- id
    4, -- aid
    5.34e-10, -- f
    null, -- d
    123, -- de
    294129412, -- i
    -784124124219410491, -- bi
    129491098649360240, --biu
    b'10000010', -- b
    '1999-03-05', -- da
    null, -- ts
    now(), -- dt
    'c', -- c
    'another another hello', -- str
    'okay, another another bye' -- t
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

insert into __test (str, id, f, d, de) values ('100', 100, 'NaN'::real, 'NaN'::double precision, 'NaN'::numeric);
insert into __test (str, id, f, d) values
    ('101', 101, '+Inf'::real, '+Inf'::double precision),
    ('102', 102, '-Inf'::real, '-Inf'::double precision);
