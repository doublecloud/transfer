create table "default-topic" (
    msg bytea,
    _timestamp timestamp without time zone,
    _partition bytea,
    _offset bigint,
    _idx integer,
    _rest jsonb default '{}',
    primary key (_partition, _offset, _idx)
);