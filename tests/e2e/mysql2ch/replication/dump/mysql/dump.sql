CREATE TABLE `mysql_replication`
(
    `id` INT AUTO_INCREMENT PRIMARY KEY,

    `val1` INT,
    `val2` VARCHAR(20),

    `b1` BIT(1),
    `b8` BIT(8),
    `b11` BIT(11)
) engine = innodb default charset = utf8;

INSERT INTO mysql_replication (id, val1, val2, b1, b8, b11) VALUES
(1, 1, 'a', b'0', b'00000000', b'00000000000'),
(2, 2, 'b', b'1', b'10000000', b'10000000000');
