create table __test
(
    id serial,
    ts timestamp,
    t  text,

    primary key (id)
);

insert into __test
values (1,
        '2001-12-10',
        'sometext'),
       (1500,
        '2001-12-10',
        '±12'),
       (34,
        '2001-12-10',
        'testtestsetset'),
       (48,
        now(),
        'hehehhehehe')
