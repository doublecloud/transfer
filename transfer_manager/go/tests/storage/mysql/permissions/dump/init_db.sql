CREATE TABLE foo
(
    id int
);

CREATE TABLE bar
(
    id int
);

CREATE USER test_user@'%' IDENTIFIED BY 'test_pass';

GRANT ALTER ON source.* TO test_user@'%';
GRANT SELECT ON source.foo TO test_user@'%';
