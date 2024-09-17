BEGIN;
CREATE TABLE tv_table(i INT, cname TEXT);
COMMIT;

BEGIN;
CREATE VIEW odd_channels AS SELECT i, cname FROM tv_table WHERE i > 2;
COMMIT;
