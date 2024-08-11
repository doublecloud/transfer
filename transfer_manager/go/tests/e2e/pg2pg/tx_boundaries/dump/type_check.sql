create table trash (trash_id serial primary key, title text);

create table __test (id serial primary key, title text);

insert into __test select s, md5(random()::text) from generate_Series(1, 50000) as s;
