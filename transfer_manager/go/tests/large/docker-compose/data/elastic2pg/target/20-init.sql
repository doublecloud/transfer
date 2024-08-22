CREATE TABLE public.test_doc (
    _id text NOT NULL,
    __data_transfer jsonb,
    data text,
    partition bigint,
    seq_no bigint,
    topic text,
    write_time timestamp without time zone
);
