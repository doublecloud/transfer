BEGIN;
CREATE TABLE __test_incremental (
    text text primary key,
    updated_at timestamp
);
COMMIT;
BEGIN;
insert into __test_incremental (text, updated_at)
select md5(random()::text), ('2020-01-01 00:00:00'::timestamp + interval '1 day' * s.s)
from generate_Series(1,2000) as s;
COMMIT;

