create table __test_parent
(
    id bigserial primary key,
    text text
);

create table __test_parent_child_1
(
    constraint constraint_1
        check ((id >= 0) AND
               (id <  100 * 1000))
) inherits (__test_parent);

create table __test_parent_child_2
(
    constraint constraint_1
        check ((id >= 100 * 1000) AND
               (id <  200 * 1000))
) inherits (__test_parent);

BEGIN;
insert into __test_parent_child_1 (text, id) select md5(random()::text), s.s from generate_series(1, 100 * 1000 - 1) as s;
COMMIT;

BEGIN;
insert into __test_parent_child_2 (text, id) select md5(random()::text), s.s from generate_series(100 * 1000, 200 * 1000 - 1) as s;
COMMIT;
