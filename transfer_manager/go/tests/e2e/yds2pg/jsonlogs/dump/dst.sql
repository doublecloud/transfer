create table "default-topic" (
    ts bytea,
    level bytea,
    caller bytea,
    msg bytea,
    _timestamp timestamp without time zone,
    _partition bytea,
    _offset bigint,
    _idx integer,
    primary key (_partition, _offset, _idx)
);