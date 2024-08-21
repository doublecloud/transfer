BEGIN;

CREATE TABLE public.__replica_id_full_1(i INT, t TEXT);
ALTER  TABLE public.__replica_id_full_1 REPLICA IDENTITY FULL;

CREATE TABLE public.__replica_id_full_2(i INT, t TEXT);
ALTER  TABLE public.__replica_id_full_2 REPLICA IDENTITY FULL;
INSERT INTO  public.__replica_id_full_2 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');

CREATE TABLE public.__replica_id_full_3(i INT, t TEXT);
ALTER  TABLE public.__replica_id_full_3 REPLICA IDENTITY FULL;
INSERT INTO  public.__replica_id_full_3 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');

CREATE TABLE public.__replica_id_full_4(i INT, t TEXT);
ALTER  TABLE public.__replica_id_full_4 REPLICA IDENTITY FULL;
INSERT INTO  public.__replica_id_full_4 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');

CREATE TABLE public.__replica_id_full_5(i INT, t TEXT);
ALTER  TABLE public.__replica_id_full_5 REPLICA IDENTITY FULL;
INSERT INTO  public.__replica_id_full_5 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');

CREATE TABLE public.__replica_id_not_full(i INT, t TEXT);

COMMIT;

BEGIN;
SELECT pg_create_logical_replication_slot('testslot1', 'wal2json');
SELECT pg_create_logical_replication_slot('testslot2', 'wal2json');
SELECT pg_create_logical_replication_slot('testslot3', 'wal2json');

SELECT pg_create_logical_replication_slot('testslot4_1', 'wal2json');
SELECT pg_create_logical_replication_slot('testslot4_2', 'wal2json');

SELECT pg_create_logical_replication_slot('testslot5_1', 'wal2json');
SELECT pg_create_logical_replication_slot('testslot5_2', 'wal2json');

SELECT pg_create_logical_replication_slot('testslot6', 'wal2json');
COMMIT;

BEGIN;
INSERT INTO public.__replica_id_full_1 VALUES (1, '111'), (2, '222');
COMMIT;

BEGIN;
DELETE FROM public.__replica_id_full_2 where i = 1;
COMMIT;

BEGIN;
UPDATE public.__replica_id_full_3 SET t = '11' where i = 1;
UPDATE public.__replica_id_full_3 SET t = '22', i = 22 where i = 2;
UPDATE public.__replica_id_full_3 SET t = '3' where i = 3;
COMMIT;

BEGIN;
INSERT INTO public.__replica_id_not_full VALUES (3, '333'), (4, '444');
COMMIT;

BEGIN;
INSERT INTO public.__replica_id_full_4 VALUES (4, '4');
COMMIT;

BEGIN;
INSERT INTO public.__replica_id_full_5 VALUES (4, '4');
COMMIT;
