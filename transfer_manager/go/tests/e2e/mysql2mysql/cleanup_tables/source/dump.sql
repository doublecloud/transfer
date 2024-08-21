create table ids_1 (
    id  int not null primary key,

    name varchar(40) not null,
    description varchar(100)
);

create table items_1 (
    id int not null primary key,
    item_id int not null,
    ts timestamp,
    city varchar(100),
    FOREIGN KEY (item_id)
        REFERENCES ids_1(id)
        ON DELETE CASCADE
);

create table ids_2 (
    id  int not null primary key,

    name varchar(40) not null,
    description varchar(100)
);

create table items_2 (
    id int not null primary key,
    item_id int not null,
    city varchar(100),
    FOREIGN KEY (item_id)
        REFERENCES ids_2(id)
        ON DELETE CASCADE
);

create view spb_items_1_2020 as
    select  *
    from items_1
    where city = 'spb' and ts >= timestamp '2020-01-01 00:00:00';

