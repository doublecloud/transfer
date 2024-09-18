DROP TABLE IF EXISTS multiple_uniq_idxs_pk;
CREATE TABLE multiple_uniq_idxs_pk(
    a INT,
    b INT,
    c_pk INT PRIMARY KEY,
    t TEXT
);
INSERT INTO multiple_uniq_idxs_pk(a, b, c_pk, t) VALUES
(1, 10, 100, 'text_1'),
(2, 20, 200, 'text_2');
CREATE UNIQUE INDEX ON multiple_uniq_idxs_pk (a) WHERE a IN (0, 2, 4, 6, 8, 10);
CREATE UNIQUE INDEX ON multiple_uniq_idxs_pk (b);
INSERT INTO multiple_uniq_idxs_pk(a, b, c_pk, t) VALUES
(3, 30, 300, 'text_3'),
(3, 40, 400, 'text_4');

DROP TABLE IF EXISTS multiple_uniq_idxs_no_complete;
CREATE TABLE multiple_uniq_idxs_no_complete(
    a INT,
    t TEXT
);
CREATE UNIQUE INDEX ON multiple_uniq_idxs_no_complete(a) WHERE a IN (0, 2, 4, 6, 8, 10);
CREATE UNIQUE INDEX ON multiple_uniq_idxs_no_complete(a) WHERE a IN (1, 3, 5, 7, 9, 11);
INSERT INTO multiple_uniq_idxs_no_complete(a, t) VALUES
(1, 'text_1'),
(2, 'text_2');
