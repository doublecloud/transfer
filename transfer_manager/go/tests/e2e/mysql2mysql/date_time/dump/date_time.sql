SET SESSION sql_mode='';

CREATE TABLE `__test1` (
    `id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `col_d` date,
    `col_dt` datetime,
    `col_ts` timestamp
) engine=innodb default charset=utf8;

CREATE TABLE `__test2` (
    `id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `col_dt1` datetime(1),
    `col_dt2` datetime(2),
    `col_dt3` datetime(3),
    `col_dt4` datetime(4),
    `col_dt5` datetime(5),
    `col_dt6` datetime(6),
    `col_ts1` timestamp(1),
    `col_ts2` timestamp(2),
    `col_ts3` timestamp(3),
    `col_ts4` timestamp(4),
    `col_ts5` timestamp(5),
    `col_ts6` timestamp(6)
) engine=innodb default charset=utf8;

INSERT INTO __test1 (col_d, col_dt, col_ts) VALUES
    ('0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00'),
    ('1000-01-01', '1000-01-01 00:00:00', '1970-01-01 00:00:01'),
    ('9999-12-31', '9999-12-31 23:59:59', '2038-01-19 03:14:07'),
    ('2020-12-23', '2020-12-23 14:15:16', '2020-12-23 14:15:16');

INSERT INTO __test2 (col_dt1, col_dt2, col_dt3, col_dt4, col_dt5, col_dt6, col_ts1, col_ts2, col_ts3, col_ts4, col_ts5, col_ts6) VALUES
    ('2020-12-23 14:15:16.1', '2020-12-23 14:15:16.12', '2020-12-23 14:15:16.123', '2020-12-23 14:15:16.1234', '2020-12-23 14:15:16.12345', '2020-12-23 14:15:16.123456','2020-12-23 14:15:16.1', '2020-12-23 14:15:16.12', '2020-12-23 14:15:16.123', '2020-12-23 14:15:16.1234', '2020-12-23 14:15:16.12345', '2020-12-23 14:15:16.123456'),
    ('2020-12-23 14:15:16.6', '2020-12-23 14:15:16.65', '2020-12-23 14:15:16.654', '2020-12-23 14:15:16.6543', '2020-12-23 14:15:16.65432', '2020-12-23 14:15:16.654321','2020-12-23 14:15:16.6', '2020-12-23 14:15:16.65', '2020-12-23 14:15:16.654', '2020-12-23 14:15:16.6543', '2020-12-23 14:15:16.65432', '2020-12-23 14:15:16.654321');