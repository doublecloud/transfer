CREATE TABLE log_table_to_be_ignored (
    id      int not null primary key,
    logdate date not null,
    msg     varchar(100)
);

--------------------------------------------------

CREATE TABLE log_table_declarative_partitioning (
    id      int not null,
    logdate date not null,
    msg     varchar(100)
) PARTITION BY RANGE (logdate);

CREATE TABLE log_table_partition_y2022m01 PARTITION OF log_table_declarative_partitioning
    FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');

CREATE TABLE log_table_partition_y2022m02 PARTITION OF log_table_declarative_partitioning
    FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');

--------------------------------------------------

CREATE TABLE log_table_inheritance_partitioning (
    id      int not null primary key,
    logdate date not null,
    msg     varchar(100)
);

CREATE TABLE log_table_descendant_y2022m01 (
    CHECK ( logdate >= DATE '2022-01-01' AND logdate < DATE '2022-02-01' )
) INHERITS (log_table_inheritance_partitioning);

CREATE TABLE log_table_descendant_y2022m02 (
    CHECK ( logdate >= DATE '2022-02-01' AND logdate < DATE '2022-03-01' )
) INHERITS (log_table_inheritance_partitioning);

CREATE OR REPLACE FUNCTION log_table_inheritance_partitioning_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.logdate >= DATE '2022-01-01' AND NEW.logdate < DATE '2022-02-01' ) THEN
        INSERT INTO log_table_descendant_y2022m01 VALUES (NEW.*);
    ELSIF ( NEW.logdate >= DATE '2022-02-01' AND NEW.logdate < DATE '2022-03-01' ) THEN
        INSERT INTO log_table_descendant_y2022m02 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the log_table_inheritance_partitioning_insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_inheritance_partitioning_trigger
    BEFORE INSERT ON log_table_inheritance_partitioning
    FOR EACH ROW EXECUTE PROCEDURE log_table_inheritance_partitioning_insert_trigger();

--------------------------------------------------

INSERT INTO log_table_declarative_partitioning(id, logdate, msg) VALUES
(0, '2022-01-01', 'msg'),
(1, '2022-01-02', 'msg'),
(2, '2022-01-03', 'msg'),
(3, '2022-01-04', 'msg'),

(4, '2022-02-01', 'msg'),
(5, '2022-02-02', 'msg'),
(6, '2022-02-03', 'msg'),
(7, '2022-02-04', 'msg');

--------------------------------------------------

INSERT INTO log_table_inheritance_partitioning(id, logdate, msg) VALUES
(0, '2022-01-01', 'msg'),
(1, '2022-01-02', 'msg'),
(2, '2022-01-03', 'msg'),
(3, '2022-01-04', 'msg'),

(4, '2022-02-01', 'msg'),
(5, '2022-02-02', 'msg'),
(6, '2022-02-03', 'msg'),
(7, '2022-02-04', 'msg');


--------------------------------------------------

INSERT INTO log_table_to_be_ignored(id, logdate, msg) VALUES
(0, '2022-01-01', 'msg'),
(1, '2022-01-02', 'msg'),
(2, '2022-01-03', 'msg'),
(3, '2022-01-04', 'msg'),

(4, '2022-02-01', 'msg'),
(5, '2022-02-02', 'msg'),
(6, '2022-02-03', 'msg'),
(7, '2022-02-04', 'msg');

