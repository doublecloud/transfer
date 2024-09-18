set @@GLOBAL.binlog_row_image = 'minimal';

CREATE TABLE `__test`
(
    `id`   INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `val1` INT,
    `val2` VARCHAR(20)
) engine = innodb
  default charset = utf8;
