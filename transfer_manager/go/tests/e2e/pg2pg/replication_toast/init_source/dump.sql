BEGIN;
CREATE SCHEMA s1;
CREATE TABLE s1.__test (
    id      integer     PRIMARY KEY,
    small   integer,
    large   text
);
ALTER TABLE s1.__test ALTER COLUMN large SET STORAGE EXTERNAL;
COMMIT;

BEGIN;
CREATE SCHEMA s2;
CREATE TABLE s2.__test (
    id      integer     PRIMARY KEY,
    small   integer,
    large   text
);
ALTER TABLE s2.__test ALTER COLUMN large SET STORAGE EXTERNAL;
COMMIT;

SELECT pg_create_logical_replication_slot('slot1', 'wal2json');
SELECT pg_create_logical_replication_slot('slot2', 'wal2json');
