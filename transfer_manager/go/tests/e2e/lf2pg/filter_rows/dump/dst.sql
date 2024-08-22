create table "default-topic" (
    msg bytea,
    id int,
    i64 bigint,
    f64 double precision,
    str text,
    b boolean,
    ts timestamp,
    nil text,
    notnil text,

    _timestamp timestamp without time zone,
    _partition bytea,
    _offset bigint,
    _idx integer,
    _rest jsonb default '{}',
    primary key (_partition, _offset, _idx)
);
