CREATE USER test_user WITH PASSWORD 'test_pass';

CREATE TABLE t_inaccessible(i INT);

CREATE TABLE t_accessible(i INT);
GRANT SELECT ON t_accessible TO test_user;

CREATE TABLE t_column_accessible(i INT, t TEXT, f FLOAT);
GRANT SELECT (i) ON t_column_accessible TO test_user;
