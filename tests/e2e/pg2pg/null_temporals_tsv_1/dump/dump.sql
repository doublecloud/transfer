create table testtable (
    id integer primary key,
    val1 timestamp without time zone,
    val2 timestamp with time zone,
    val3 date
);
insert into testtable values (1, NULL, NULL, NULL);
