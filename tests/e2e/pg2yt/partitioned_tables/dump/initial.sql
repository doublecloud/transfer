------------------------------------
-- Partitioning Using Inheritance
------------------------------------

CREATE TABLE measurement_inherited (
    id              int not null,
    logdate         date not null,
    unitsales       int,
    PRIMARY KEY (id, logdate)
);

CREATE TABLE measurement_inherited_y2006m02 (
	CHECK ( logdate >= DATE '2006-02-01' AND logdate < DATE '2006-03-01' )
) INHERITS (measurement_inherited);

CREATE TABLE measurement_inherited_y2006m03 (
	CHECK ( logdate >= DATE '2006-03-01' AND logdate < DATE '2006-04-01' )
) INHERITS (measurement_inherited);

CREATE TABLE measurement_inherited_y2006m04 (
        CHECK ( logdate >= DATE '2006-04-01' AND logdate < DATE '2006-05-01' )
) INHERITS (measurement_inherited);

ALTER TABLE measurement_inherited_y2006m02 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_inherited_y2006m03 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_inherited_y2006m04 ADD PRIMARY KEY (id, logdate);

CREATE RULE measurement_inherited_insert_y2006m02 AS
ON INSERT TO measurement_inherited WHERE
    ( logdate >= DATE '2006-02-01' AND logdate < DATE '2006-03-01' )
DO INSTEAD
    INSERT INTO measurement_inherited_y2006m02 VALUES (NEW.*);

CREATE RULE measurement_inherited_insert_y2006m03 AS
ON INSERT TO measurement_inherited WHERE
    ( logdate >= DATE '2006-03-01' AND logdate < DATE '2006-04-01' )
DO INSTEAD
    INSERT INTO measurement_inherited_y2006m03 VALUES (NEW.*);

CREATE RULE measurement_inherited_insert_y2006m04 AS
ON INSERT TO measurement_inherited WHERE
    ( logdate >= DATE '2006-04-01' AND logdate < DATE '2006-05-01' )
DO INSTEAD
    INSERT INTO measurement_inherited_y2006m04 VALUES (NEW.*);

INSERT INTO measurement_inherited(id, logdate, unitsales)
VALUES
(1, '2006-02-02', 1),
(2, '2006-02-02', 1),
(3, '2006-03-03', 1),
(4, '2006-03-03', 1),
(5, '2006-03-03', 1),
(10, '2006-04-03', 1),
(11, '2006-04-03', 1),
(12, '2006-04-03', 1);

------------------------------------
-- Declarative Partitioning
------------------------------------

CREATE TABLE measurement_declarative (
    id         int not null,
    logdate    date not null,
    unitsales  int
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_declarative_y2006m02 PARTITION OF measurement_declarative
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');
CREATE TABLE measurement_declarative_y2006m03 PARTITION OF measurement_declarative
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');
CREATE TABLE measurement_declarative_y2006m04 PARTITION OF measurement_declarative
    FOR VALUES FROM ('2006-04-01') TO ('2006-05-01');

ALTER TABLE measurement_declarative_y2006m02 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_declarative_y2006m03 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_declarative_y2006m04 ADD PRIMARY KEY (id, logdate);

INSERT INTO measurement_declarative(id, logdate, unitsales)
VALUES
(1, '2006-02-02', 1),
(2, '2006-02-02', 1),
(3, '2006-03-03', 1),
(4, '2006-03-03', 1),
(5, '2006-03-03', 1),
(10, '2006-04-03', 1),
(11, '2006-04-03', 1),
(12, '2006-04-03', 1);
