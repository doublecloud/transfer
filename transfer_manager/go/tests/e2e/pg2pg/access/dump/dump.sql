-- This test checks access is properly checked at Upload

CREATE USER blockeduser PASSWORD 'sim-sim@OPEN';

CREATE TABLE t_accessible(i INT PRIMARY KEY, t TEXT);
INSERT INTO t_accessible VALUES (1, 'a'), (2, 'b'), (3, 'c');
GRANT SELECT ON TABLE t_accessible TO blockeduser;

CREATE TABLE t_empty(LIKE t_accessible);
GRANT SELECT ON TABLE t_empty TO blockeduser;

CREATE TABLE t_inaccessible(LIKE t_accessible);
INSERT INTO t_inaccessible SELECT * FROM t_accessible;
REVOKE SELECT ON TABLE t_inaccessible FROM blockeduser;

CREATE TYPE custom_enum AS ENUM('a', 'b', 'c');
