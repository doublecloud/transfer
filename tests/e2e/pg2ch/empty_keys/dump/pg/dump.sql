-- needs to be sure there is db1
create table __test
(
    id   int,
    val1 int,
    val2 varchar
);

insert into __test (id, val1, val2)
values (1, 1, 'some'),
       (2, 1, 'string'),
       (2, 2, 'values'),
       (3, 4, 'values'),
       (4, 3, 'here'),
       (null, 3, 'here'),
       (4, null, 'here'),
       (4, 3, null)
