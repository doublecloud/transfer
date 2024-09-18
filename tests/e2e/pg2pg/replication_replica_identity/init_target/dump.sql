CREATE TABLE public.__replica_id_full_1(i INT, t TEXT);
CREATE TABLE public.__replica_id_full_2(i INT, t TEXT);
CREATE TABLE public.__replica_id_full_3(i INT, t TEXT);
CREATE TABLE public.__replica_id_full_4(i INT, t TEXT);
CREATE TABLE public.__replica_id_full_5(i INT, t TEXT);

INSERT INTO public.__replica_id_full_2 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');
INSERT INTO public.__replica_id_full_3 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');
INSERT INTO public.__replica_id_full_4 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');
INSERT INTO public.__replica_id_full_5 (i, t) VALUES (1, '1'), (2, '2'), (3, '3');

-- Set full replica identity, otherwise checksum will return error on schema comparison
-- i.e. primary keys on source and target do not match
ALTER TABLE public.__replica_id_full_1 REPLICA IDENTITY FULL;
ALTER TABLE public.__replica_id_full_2 REPLICA IDENTITY FULL;
ALTER TABLE public.__replica_id_full_3 REPLICA IDENTITY FULL;
ALTER TABLE public.__replica_id_full_4 REPLICA IDENTITY FULL;
ALTER TABLE public.__replica_id_full_5 REPLICA IDENTITY FULL;

CREATE TABLE public.__replica_id_not_full(i INT, t TEXT);
