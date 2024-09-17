BEGIN;
SELECT pg_create_logical_replication_slot('testslot', 'wal2json');
COMMIT;

BEGIN;
CREATE TABLE tv_table(i INT PRIMARY KEY, cname TEXT);
COMMIT;

BEGIN;
CREATE VIEW odd_channels AS SELECT i, cname FROM tv_table WHERE i > 2;
COMMIT;

BEGIN;
INSERT INTO tv_table(i, cname) VALUES (1, 'ZDF');
INSERT INTO tv_table(i, cname) VALUES (2, 'Das Erste');
INSERT INTO tv_table(i, cname) VALUES (3, 'RTL');
INSERT INTO tv_table(i, cname) VALUES (4, 'SAT.1');
INSERT INTO tv_table(i, cname) VALUES (5, 'VOX');
COMMIT;
