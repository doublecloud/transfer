CREATE TABLE `__test` (
    `int`         INT,
    `int_u`       INT UNSIGNED,

    `bool`        BOOL,

    `char`        CHAR(10),
    `varchar`     VARCHAR(20),

    `id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY         -- just to have a primary key
) engine=innodb default charset=utf8;

INSERT INTO `__test`
(`int`, `int_u`, `bool`, `char`, `varchar`)
VALUES
(1, 2, true, 'text', 'test')
,
(-123, 234, false, 'magic', 'string')
;
