CREATE TABLE `__test_A` (
    `a_id` integer NOT NULL PRIMARY KEY,
    `a_name` varchar(255) NOT NULL
) engine=innodb default charset=utf8;

CREATE TABLE `__test_B` (
    `b_id` integer NOT NULL PRIMARY KEY,
    `b_name` varchar(255) NOT NULL,
    `b_address` varchar(255) NOT NULL
) engine=innodb default charset=utf8;

CREATE TABLE `__test_C` (
    `c_id` integer NOT NULL,
    `c_uid` integer NOT NULL,
    `c_name` varchar(255) NOT NULL,
    PRIMARY KEY(`c_id`, `c_uid`)
) engine=innodb default charset=utf8;

CREATE TABLE `__test_D` (
    `d_id` int NOT NULL PRIMARY KEY,
    `d_uid` bigint,
    `d_name` varchar(255)
) engine=innodb default charset=utf8;
