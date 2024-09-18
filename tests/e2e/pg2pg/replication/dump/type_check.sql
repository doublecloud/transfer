-- needs to be sure there is db1
create table __test
(
    id  bigint not null,
    aid serial,
    bid bigserial,
    si  smallint,
    ss  smallserial,

    uid uuid,

    bl  boolean,

    -- numeric
    f   float,
    d   double precision,
    de  decimal(10, 2),
--     ti   tinyint,
--     mi   mediumint,
    i   int,
    bi  bigint,
    biu bigint,
    b   bit(8),
    vb  varbit(8),

    -- date time
    da  date,
    ts  timestamp,
    dt  timestamp,
    tst timestamp with time zone,
    iv  interval,
    tm  time without time zone,
--     tt  time with time zone,
--     y    year,

    -- strings
    c   char,
    str varchar(256),
    t   text,
--     bb   blob,

    -- binary
    ba  bytea,
--     bin  binary(10),
--     vbin varbinary(100),

    -- addresses
    cr  cidr,
    it  inet,
    ma  macaddr,

    -- geometric types
    bx  box,
    cl  circle,
    ln  line,
    ls  lseg,
    ph  path,
    pt  point,
    pg  polygon,

    -- text search
--     tq  tsquery,
--     tv  tsvector,

--     tx  txid_snapshot,

    -- other
--     e    enum ("e1", "e2"),
--     se   set('a', 'b', 'c'),
    j   json,
    jb  jsonb,
    x   xml,
    arr int[],
--     gi  int generated always as identity,
--     pl   pg_lsn
    primary key (aid, str, id) -- test multi pk and reverse order keys
);

insert into __test
values (1,
        0,
        9223372036854775807,
        -32768,
        1,
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
        false,
        1.45e-10,
        3.14e-100,
        2.5,
--     -124,  -- ti
--     32765, -- mi
        -8388605,
        2147483642,
        9223372036854775804,
        b'10101111',
        b'10101111',
        '2005-03-04',
        now(),
        now(),
        '2004-10-19 10:23:54+02',
        interval '1 day 01:00:00',
        '04:05:06.789',
--         '04:05:06 PST',
--    '04:05:06.789',
--    '2099',  -- year

        '1',
        'hello, friend of mine',
        'okay, now bye-bye',
--     'this it actually text but blob', -- blob

        decode('CAFEBABE', 'hex'),
--     'a\0deadbeef',  -- bin
--     'cafebabe',     -- vbin

        '192.168.100.128/25',
        '192.168.100.128/25',
        '08:00:2b:01:02:03',
        box(circle '((0,0),2.0)'),
        circle(box '((0,0),(1,1))'),
        line(point '(-1,0)', point '(1,0)'),
        lseg(box '((-1,0),(1,0))'),
        path(polygon '((0,0),(1,1),(2,0))'),
        point(23.4, -44.5),
        polygon(box '((0,0),(1,1))'),

--     to_tsquery('cat' & 'rat'),
--     to_tsvector('fat cats ate rats'),

--     txid_current_snapshot(),

--     "e1",           -- e
--     'a',            -- se
        '{
          "yandex is the best place to work at": [
            "wish i",
            "would stay",
            4.15,
            {
              "here after": "the  "
            },
            [
              "i",
              [
                "n",
                [
                  "t",
                  "e r n s h i"
                ],
                "p"
              ]
            ]
          ]
        }',
        '{
          "yandex is the best place to work at": [
            "wish i",
            "would stay",
            4.15,
            {
              "here after": "the  "
            },
            [
              "i",
              [
                "n",
                [
                  "t",
                  "e r n s h i"
                ],
                "p"
              ]
            ]
          ]
        }',
        '
        <foo>bar</foo>',
        '{1, 2, 3}'
--     '68/1225BB70'
       )
        ,
       (2,
        1,
        9223372036854775806,
        32767,
        32767,
        'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
        true,
        1.34e-10,
        null,
        null,
--     -12,  -- ti
--     1123, -- mi
        -1294129412,
        112412412421941041,
        129491244912401240,
        b'10000001',
        b'10000001',
        '1999-03-04',
        now(),
        null,
        'Wed Dec 17 07:37:16 1997 PST',
        interval '-23:00:00',
        '040506',
--         '2003-04-12 04:05:06 America/New_York',
--     '04:05 PM',
--     '1971', -- year

        '2',
        'another hello',
        'okay, another bye',
--     'another blob', -- blob

        'well, I got stuck with time and it took a huge amount of time XD',
--     'cafebabeda',   -- bin
--     '\0\0\0\0\1',   -- vbin

        '192.168/24',
        '192.168.0.0/24',
        '08-00-2b-01-02-03',
        box(point '(0,0)'),
        circle(point '(0,0)', 2.0),
        line(point '(-2,0)', point '(2,0)'),
        lseg(point '(-1,0)', point '(1,0)'),
        path(polygon '((0,0),(1,0),(1,1),(0,1))'),
        point(box '((-1,0),(1,0))'),
        polygon(circle '((0,0),2.0)'),

--     to_tsquery(('(fat | rat) & cat'),
--     to_tsvector('a:1 b:2 c:1 d:2 b:3'),

--     txid_current_snapshot(),

--     "e2",           -- e
--     'b',            -- se
        '{
          "simpler": [
            "than",
            13e-10,
            {
              "it": {
                "could": "be"
              }
            }
          ]
        }',
        '{
          "simpler": [
            "than",
            13e-10,
            {
              "it": {
                "could": "be"
              }
            }
          ]
        }',
        '
        <root>
            <smile>
                <straw>I am new</straw>
                <couple>intern at TM team.</couple>
                <health>TM team is</health>
                <sound>the</sound>
                <dog>best</dog>
                <diagram>team.</diagram>
            </smile>
            <metal>hazzus</metal>
            <setting>you</setting>
            <cotton>were</cotton>
            <my>absolutely</my>
            <whole>right</whole>
        </root>',
        NULL
--     '0/0'
       )
        ,
       (3,
        4,
        9223372036854775805,
        13452,
        -12345,
        'a0eebc999c0b4ef8bb6d6bb9bd380a11',
        false,
        5.34e-10,
        null,
        123,
--     -122,  -- ti
--     -1123, -- mi
        294129412,
        -784124124219410491,
        129491098649360240,
        b'10000010',
        b'10000010',
        '1999-03-05',
        null,
        now(),
        '12/17/1997 07:37:16.00 PST',
        interval '21 days',
        '04:05 PM',
--         '21:32:12 PST',
--     '04:05-08:00',
--     '1972', -- year

        'c',
        'another another hello',
        'okay, another another bye',
--     'another another blob but looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo'
--     'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
--     'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
--     'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
--     'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
--     'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
--     'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
--     'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
--     'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg'
--     'nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn'
--     'ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg',  -- blob

        'john is gonna dance jaga-jaga',
--     'caafebabee',           -- bin
--     '\0\0\0\0\1abcd124edb', -- vbin

        '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128',
        '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128',
        '08002b010203',
        box(point '(0,0)', point '(1,1)'),
        circle(polygon '((0,0),(1,1),(2,0))'),
        line(point '(-3,0)', point '(3,0)'),
        lseg(box '((-2,0),(2,0))'),
        path(polygon '((0,0),(1,1),(2,3),(3,1),(4,0))'),
        point(circle '((0,0),2.0)'),
        polygon(12, circle '((0,0),2.0)'),

--     to_tsquery('fat' <-> 'rat'),
--     array_to_tsvector('{fat,cat,rat}'::text[]),

--     txid_current_snapshot(),

--     "e1",                   -- e
--     'c',                    -- se
        '{
          "simpler": [
            "than",
            13e-10,
            {
              "it": {
                "could": [
                  "be",
                  "no",
                  "ideas    ",
                  "   again"
                ],
                "sorry": null
              }
            }
          ]
        }',
        '{
          "simpler": [
            "than",
            13e-10,
            {
              "it": {
                "could": [
                  "be",
                  "no",
                  "ideas    ",
                  "   again"
                ],
                "sorry": null
              }
            }
          ]
        }',
        '
        <root>
            <occur>1465580861.7786624</occur>
            <dish>lady</dish>
            <chose>
                <flight>-695149882.8150392</flight>
                <bound>voice</bound>
                <smile>
                    <thee>throat</thee>
                    <sheep>saw</sheep>
                    <finger>silk</finger>
                    <save>accident</save>
                    <somebody>-1524256040.2926793</somebody>
                    <window>1095844440</window>
                </smile>
                <chart>-2013145083.260986</chart>
                <necessary>element</necessary>
                <guess>-1281358606.1880667</guess>
            </chose>
            <board>2085211696</board>
            <crop>-748870413</crop>
            <bowl>986627174</bowl>
        </root>',
        NULL
--     '0/0'
       )
;

insert into __test (str, id)
values ('hello', 0),
       ('aaa', 214),
       ('vvvv', 124124),
       ('agpnaogapoajfqt-oqoo ginsdvnaojfspbnoaj apngpowo qeonwpbwpen', 1234),
       ('aagiangsfnaofasoasvboas', 12345);

insert into __test (str, id, da)
values ('nvaapsijfapfn', 201, now()),
       ('Day the creator of this code was born', 202, '1999-09-16'),
       ('Coronavirus made me leave', 322, '2020-06-03'),
       ('But Ill be back, this is public promise', 422, now()),
       ('Remember me, my name is hazzus', 333, now());

alter table __test replica identity full;

-- insert into __test (id, str, mi) values (2020, 'thanks for everything, my team', 5),
--                     (2019, 'and other guys I worked with', 5);
