BEGIN;
CREATE TABLE __test (
    id     integer  PRIMARY KEY,
    value  text
);
INSERT INTO __test VALUES (1, 'a');
COMMIT;

SELECT pg_create_logical_replication_slot('testslot', 'wal2json');

UPDATE __test SET id = 2 WHERE id = 1;
INSERT INTO __test VALUES (3, 'c');
