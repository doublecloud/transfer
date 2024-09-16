CREATE TABLE __test(
       a INT,
       b INT
);

INSERT INTO __test(a, b) VALUES (1,2), (3,4), (4,5);
alter table __test replica identity full;
