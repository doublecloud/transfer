-- needs to be sure there is db1
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
    ts   timestamp default '1999-03-06'::timestamp,
    dt   timestamp,
    c    char,
    str  varchar(256),
    t    text,
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
   '1999-03-06',
   '2005-03-12',
    '1',
    'hello, friend of mine',
    'okay, now bye-bye'
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
    '1999-03-06',
    null,
    '2',
    'another hello',
    'okay, another bye'
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
    '1999-03-06',
    '2005-03-12',
    'c',
    'another another hello',
    'okay, another another bye'
)
;

insert into __test (str, id) values ('hello', 0),
                                    ('aaa', 214),
                                    ('vvvv', 124124),
                                    ('agpnaogapoajfqt-oqoo ginsdvnaojfspbnoaj apngpowo qeonwpbwpen', 1234),
                                    ('aagiangsfnaofasoasvboas', 12345);

insert into __test (str, id, da) values ('nvaapsijfapfn', 201, '2005-03-12'),
                                        ('Day the creator of this code was born', 202, '1999-09-16'),
                                        ('Coronavirus made me leave', 322, '2020-06-03'),
                                        ('But Ill be back, this is public promise', 422, '2005-03-12'),
                                        ('Remember me, my name is hazzus', 333, '2005-03-12');

insert into __test (str, id, f, d, de) values ('100', 100, 'NaN'::real, 'NaN'::double precision, 'NaN'::numeric);
insert into __test (str, id, f, d) values
    ('101', 101, '+Inf'::real, '+Inf'::double precision),
    ('102', 102, '-Inf'::real, '-Inf'::double precision);
