BEGIN;
create table testtable (
    id integer primary key,
    val jsonb
);
COMMIT;
