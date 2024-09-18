CREATE TABLE `test_src` (
    `id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name` varchar(10),
    UNIQUE (`name`)
) engine=innodb default charset=utf8;

CREATE TABLE `test_dst` (
    `src_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
    FOREIGN KEY (`src_id`) REFERENCES `test_src` (`id`)
) engine=innodb default charset=utf8;

INSERT INTO `test_src` VALUES (1, 'test');
INSERT INTO `test_dst` VALUES (1);
