create table __test(
   id serial primary key ,
   val text,
   created_at timestamp default CURRENT_TIMESTAMP not null
);

insert into __test (val)
values ('1'), ('2'), ('3');
