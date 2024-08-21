-- needs to be sure there is db1
create table __test_1
(
    id   int,
    val1 int,
    val2 varchar,
    primary key (id)
);

insert into __test_1 (id, val1, val2)
values (1, 1, 'a'),
       (2, 2, 'b');

create table __test_2
(
    id   int,
    val1 int,
    val2 varchar,
    primary key (id)
);

insert into __test_2 (id, val1, val2)
values (1, 2, 'b'),
       (2, 2, 'c');

create table __test_3
(
    id   int,
    val1 int,
    val2 varchar,
    primary key (id)
);

insert into __test_3 (id, val1, val2)
values (1, 3, 'c'),
       (2, 2, 'd');

