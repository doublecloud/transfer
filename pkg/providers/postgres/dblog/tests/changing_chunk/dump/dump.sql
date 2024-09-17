CREATE TABLE __test_num_table
(
    id INT PRIMARY KEY,
    num INT
);

INSERT INTO __test_num_table (id, num)
SELECT gs, gs
FROM generate_series(1, 10) as gs;
