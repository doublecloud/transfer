create table ids_1 (
    id  int not null primary key,

    name varchar(40) not null,
    description varchar(100)
);

create sequence ids_1_seq as int increment by 1
owned by ids_1.id;

create table items_1 (
    id int not null primary key,
    item_id int not null references ids_1(id),
    ts timestamp,
    city varchar(100)
);
create sequence items_1_seq as int increment by 1
owned by items_1.id;

create table ids_2 (
    id  int not null primary key,

    name varchar(40) not null,
    description varchar(100)
);

create sequence ids_2_seq as int increment by 1
owned by ids_2.id;

create table items_2 (
    id int not null primary key,
    item_id int not null references ids_2(id),
    city varchar(100)
);
create sequence items_2_seq as int increment by 1
owned by items_2.id;

create view spb_items_1_2020 as
    select  *
    from items_1
    where city = 'spb' and ts >= timestamp '2020-01-01 00:00:00';
