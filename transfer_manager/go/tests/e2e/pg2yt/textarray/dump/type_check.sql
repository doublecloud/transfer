create table test (
  id int primary key,
  values text[]
);
insert into test values (1, '{"asd","adsa"}');
insert into test values (2, '{"dsa","adsa,dsadas,dsada,sd"}');
