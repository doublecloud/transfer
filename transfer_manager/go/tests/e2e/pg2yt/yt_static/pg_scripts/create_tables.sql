CREATE TABLE test_table
(
    id int
);

INSERT INTO test_table
SELECT id
FROM generate_series(1, 100) AS t(id);