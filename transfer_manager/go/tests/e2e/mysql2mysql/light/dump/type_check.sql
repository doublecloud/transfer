-- needs to be sure there is db1
create table __test (
    id  bigint(64) unsigned  not null,
    aid integer unsigned auto_increment,

    -- numeric
    f    float,
    d    double,
    de   decimal(10,2),
    ti   tinyint,
    mi   mediumint,
    i    int,
    bi   bigint,
    biu  bigint unsigned,
    b    bit(8),

    -- date time
    da   date,
    ts   timestamp,
    dt   datetime,
    tm   time,
    y    year,

    -- strings
    c    char,
    str  varchar(256),
    t    text,
    bb   blob,

    -- binary
    bin  binary(10),
    vbin varbinary(100),

    -- other
    e    enum ("e1", "e2"),
    se   set('a', 'b', 'c'),
    j    json,
    primary key (aid, str, id) -- test multi pk and reverse order keys
) engine=innodb default charset=utf8;

insert into __test values (
    1,
    0,
    1.45e-10,
    3.14e-100,
    2.5,
    -124,
    32765,
    -8388605,
    2147483642,
    9223372036854775804,

    b'10101111',

    '2005-03-04',
   now(),
   now(),
   now(),
   '2099',

    '1',
    'hello, friend of mine',
    'okay, now bye-bye',
    'this it actually text but blob',
    'a\0deadbeef',
    'cafebabe',
    "e1",
    'a',
    '{"yandex is the best place to work at": ["wish i", "would stay", 4.15, {"here after":"the  "}, ["i", ["n", ["t", "e r n s h i"], "p"]]]}'
), (
    2,
    1,
    1.34e-10,
    null,
    null,
    -12,
    1123,
    -1294129412,
    112412412421941041,
    129491244912401240,

    b'10000001',

    '1999-03-04',
    now(),
    null,
    now(),
    '1971',

    '2',
    'another hello',
    'okay, another bye',
    'another blob',
    'cafebabeda',
    '\0\0\0\0\1',
    "e2",
    'b',
    '{"simpler": ["than", 13e-10, {"it": {"could": "be"}}]}'
), (
    3,
    4,
    5.34e-10,
    null,
    123,
    -122,
    -1123,
    294129412,
    -784124124219410491,
    129491098649360240,

    b'10000010',

    '1999-03-05',
    null,
    now(),
    now(),
    '1972',

    'c',
    'another another hello',
    'okay, another another bye',
    'another another blob but looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo'
    'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
    'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
    'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
    'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
    'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
    'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
    'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
    'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
    'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
    'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg',
    'caafebabee',
    '\0\0\0\0\1abcd124edb',
    "e1",
    'c',
    '{"simpler": ["than", 13e-10, {"it": {"could": ["be", "no", "ideas    ", "   again"], "sorry": null}}]}'
);

insert into __test (str, id) values ('hello', 0),
                                    ('aaa', 214),
                                    ('vvvv', 124124),
                                    ('agpnaogapoajfqt-oqoo ginsdvnaojfspbnoaj apngpowo qeonwpbwpen', 1234),
                                    ('aagiangsfnaofasoasvboas', 12345);

insert into __test (str, id, da) values ('nvaapsijfapfn', 201, now()),
                                        ('Day the creator of this code was born', 202, '1999-09-16'),
                                        ('Coronavirus made me leave', 322, '2020-06-03'),
                                        ('But I\'ll be back, this is public promise', 422, now()),
                                        ('Remember me, my name is hazzus', 333, now());

insert into __test (id, str, mi) values (2020, 'thanks for everything, my team', 5),
                                        (2019, 'and other guys I worked with', 5);

insert into __test (id, j) values (3000, '{"\\"": "\\\\", "''": []}'); -- JSON: {"\"": "\\", "'": []}
