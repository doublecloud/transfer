CREATE TABLE `__test`
(
    `id`    INT PRIMARY KEY,
    `value` text
) engine = innodb
  default charset = utf8;

INSERT INTO `__test`
    (`id`, `value`)
VALUES (1, 'test')
        ,
       (2, 'magic')
;

CREATE TABLE `__test_composite_pkey`
(
    `id`    INT,
    `id2`   INT,
    `value` text,
     PRIMARY KEY(`id2`, `id`)
) engine = innodb
  default charset = utf8;

INSERT INTO `__test_composite_pkey`
    (`id`, `id2`, `value`)
VALUES (1, 12, 'test')
        ,
       (2, 22, 'magic')
;
