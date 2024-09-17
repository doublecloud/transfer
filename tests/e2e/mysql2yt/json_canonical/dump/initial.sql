CREATE TABLE `test_snapshot_and_increment`
(
    `id`        INTEGER PRIMARY KEY,
    `jsoncol`   JSON
);

CREATE TABLE `test_increment_only`
(
    `id`        INTEGER PRIMARY KEY,
    `jsoncol`   JSON NOT NULL
);

INSERT INTO `test_snapshot_and_increment` (`id`, `jsoncol`)
VALUES
    (0, JSON_OBJECT('hello', 'world')),
    (1, CAST('123' AS JSON)),
    (2, CAST(123 AS JSON)),
    (3, JSON_ARRAY()),
    (4, JSON_ARRAY('abyr')),
    (5, JSON_ARRAY(123, 'abyr', JSON_ARRAY('valg'), JSON_OBJECT('kek', CAST(999999999999999999999999999999.000000000000000000000000000000000000000000001 as JSON), 'lel', 777))),
    (6, CAST(1234567890123456789012345678901234567890.123456789012345678901234567890123456 AS JSON)),
    (7, CAST(NULL AS JSON)),
    (8, JSON_OBJECT('kek', CAST(NULL AS JSON))),
    (9, JSON_QUOTE('"string in quotes"')) -- In JSON it's "\"string in quotes\""
;
