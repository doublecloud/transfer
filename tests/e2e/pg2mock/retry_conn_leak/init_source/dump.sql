BEGIN;
    CREATE TABLE __test1 (
         id integer PRIMARY KEY,
         value text
    );
COMMIT;
BEGIN;
    SELECT pg_create_logical_replication_slot('testslot', 'wal2json');
COMMIT;
BEGIN;
    insert into __test1 (id, value) values (1, 'test');
COMMIT;
