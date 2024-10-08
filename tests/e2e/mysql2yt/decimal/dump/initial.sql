CREATE TABLE `test_snapshot_and_increment`
(
    `id`        INTEGER PRIMARY KEY,
    `value`     DECIMAL(65, 30),
    `value_10`  DECIMAL(10, 0)
);

CREATE TABLE `test_increment_only`
(
    `id`        INTEGER PRIMARY KEY,
    `value`     DECIMAL(65, 30),
    `value_10`  DECIMAL(10, 0)
);

INSERT INTO `test_snapshot_and_increment` (`id`, `value`, `value_10`)
VALUES
    (0,  0,                                                                   0),
    (1,  9999999999999999999999999999999999,                                  99999),
    (2,  99999999999999999999999999999999999,                                 9999999),
    (3,  9999999999999999999999999999999999999999999999999999999999999999,    9999999999),
    (4,  99999999999999999999999999999999999999999999999999999999999999999,   9999999999),
    (5,  999999999999999999999999999999999999.99999999999999999999999999999,  9999999999),
    (6,  99999999999999999999999999999999999.999999999999999999999999999999,  9999999999),
    (7,  1.000000000000000000000000000001,                                    1),
    (8,  NULL,                                                                9999999999),
    (9,  99999999999999999999999999999999999.999999999999999999999999999999,  NULL)
;
