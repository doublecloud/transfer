CREATE TABLE test (
    aid    integer   PRIMARY KEY,
    bid    integer,
    value  text
);
CREATE UNIQUE INDEX uindex ON test (bid);
