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
