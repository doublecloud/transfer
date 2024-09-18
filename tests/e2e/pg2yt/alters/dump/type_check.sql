create table __test_a
(
    a_id   integer      not null primary key,
    a_name varchar(255) not null
);

create table __test_b
(
    b_id      integer      not null primary key,
    b_name    varchar(255) not null,
    b_address varchar(255) not null
);

create table __test_c
(
    c_id   integer      not null primary key,
    c_uid  integer      not null,
    c_name varchar(255) not null
);

create table __test_d
(
    d_id   int not null primary key,
    d_uid  bigint,
    d_name varchar(255)
);

insert into __test_a (a_id, a_name)
values (1, 'jagajaga'),
       (2, 'bamboo');

insert into __test_b (b_id, b_name, b_address)
values (1, 'Mike', 'Pushkinskaya, 1'),
       (2, 'Rafael', 'Ostankinskaya, 8');

insert into __test_c (c_id, c_uid, c_name)
values (1, 9, 'Macbook Pro, 15'),
       (2, 4, 'HP Pavilion');

insert into __test_d (d_id, d_uid, d_name)
values (1, 13, 'Reverse Engineering'),
       (2, 37, 'Evolutionary Computations');
