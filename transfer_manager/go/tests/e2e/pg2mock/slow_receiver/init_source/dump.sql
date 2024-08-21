BEGIN;
CREATE TABLE __test1 (
    id integer PRIMARY KEY,
    value text
);
CREATE TABLE __test2 (
    id integer PRIMARY KEY,
    value text
);
COMMIT;
SELECT pg_create_logical_replication_slot('testslot', 'wal2json');
