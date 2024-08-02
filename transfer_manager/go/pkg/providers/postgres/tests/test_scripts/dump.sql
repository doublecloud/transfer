BEGIN;
CREATE TABLE __test_to_shard (
    id bigserial primary key,
    text text
);
COMMIT;
BEGIN;
insert into __test_to_shard (text) select md5(random()::text) from generate_Series(1,100000) as s;
COMMIT;

BEGIN;
CREATE TABLE __test_to_shard_int32 (
                                 "Id" serial primary key,
                                 text text
);
COMMIT;
BEGIN;
insert into __test_to_shard_int32 (text) select md5(random()::text) from generate_Series(1,100000) as s;
COMMIT;

BEGIN;
CREATE TABLE __test_incremental (
    text text primary key,
    cursor integer
);
COMMIT;
BEGIN;
insert into __test_incremental (text, cursor) select md5(random()::text), s.s from generate_Series(1,10) as s;
COMMIT;


CREATE TABLE __test_incremental_ts (
    text text primary key,
    cursor timestamp
);
COMMIT;
BEGIN;
insert into __test_incremental_ts (text, cursor) select md5(random()::text), now() from generate_Series(1,10) as s;
COMMIT;

