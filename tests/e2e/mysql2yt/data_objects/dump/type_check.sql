create table `__not_included_test`
(
    `id`    INT PRIMARY KEY,
    `value` text
) engine = innodb
    default charset = utf8;

INSERT INTO `__not_included_test`
(`id`, `value`)
VALUES (1, 'not_included_test')
;


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
