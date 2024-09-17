-- MySQL permits you to store a “zero” value of '0000-00-00' as a “dummy date.”

-- For DATETIME, DATE, and TIMESTAMP types, MySQL interprets dates specified with ambiguous year values using these rules:
-- Year values in the range 00-69 become 2000-2069.
-- Year values in the range 70-99 become 1970-1999.

create table if not exists date_types
(
    __primary_key INT PRIMARY KEY,

    --

    t_date             DATE, -- The supported range is '1000-01-01' to '9999-12-31'

    t_year             YEAR, -- https://dev.mysql.com/doc/refman/8.0/en/year.html
    t_year4            YEAR(4), -- it's alias to year

    t_timestamp        TIMESTAMP DEFAULT '0000-00-00',
    t_timestamp0       TIMESTAMP(0),
    t_timestamp1       TIMESTAMP(1),
    t_timestamp2       TIMESTAMP(2),
    t_timestamp3       TIMESTAMP(3),
    t_timestamp4       TIMESTAMP(4),
    t_timestamp5       TIMESTAMP(5),
    t_timestamp6       TIMESTAMP(6),

    t_time             TIME, -- TIME values may range from '-38:59:59' to '38:59:59'
    t_time0            TIME(0),
    t_time1            TIME(1),
    t_time2            TIME(2),
    t_time3            TIME(3),
    t_time4            TIME(4),
    t_time5            TIME(5),
    t_time6            TIME(6),

    t_datetime         DATETIME, -- The supported range is '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.
    t_datetime0        DATETIME(0),
    t_datetime1        DATETIME(1),
    t_datetime2        DATETIME(2),
    t_datetime3        DATETIME(3),
    t_datetime4        DATETIME(4),
    t_datetime5        DATETIME(5),
    t_datetime6        DATETIME(6)
);

INSERT INTO date_types (__primary_key, t_date) VALUES (10000, '1000-01-01');
INSERT INTO date_types (__primary_key, t_date) VALUES (10001, '9999-12-31');

INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11000, '1901', '1901');
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11001, '2155', '2155');
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11002, 1901, 1901);
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11003, 1901, 1901);
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11004, '0', '0');
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11005, '69', '69');
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11006, '70', '70');
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11007, '99', '99');
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11008, 0, 0);
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11009, 69, 69);
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11010, 70, 70);
INSERT INTO date_types (__primary_key, t_year, t_year4) VALUES (11011, 99, 99);

INSERT INTO date_types (__primary_key, t_timestamp,          t_timestamp0,         t_timestamp1,           t_timestamp2,            t_timestamp3,             t_timestamp4,              t_timestamp5,               t_timestamp6) VALUES (12000,
                                       '1970-01-01 00:00:01','1970-01-01 00:00:01','1970-01-01 00:00:01.1','1970-01-01 00:00:01.12','1970-01-01 00:00:01.123','1970-01-01 00:00:01.1234','1970-01-01 00:00:01.12345','1970-01-01 00:00:01.123456');
INSERT INTO date_types (__primary_key, t_timestamp,          t_timestamp0,         t_timestamp1,           t_timestamp2,            t_timestamp3,             t_timestamp4,              t_timestamp5,               t_timestamp6) VALUES (12001,
                                       '2038-01-19 03:14:07','2038-01-19 03:14:07','2038-01-19 03:14:07.1','2038-01-19 03:14:07.12','2038-01-19 03:14:07.123','2038-01-19 03:14:07.1234','2038-01-19 03:14:07.12345','2038-01-19 03:14:07.123456');

INSERT INTO date_types (__primary_key, t_time,      t_time0,     t_time1,       t_time2,        t_time3,         t_time4,          t_time5,           t_time6) VALUES (13000,
                                       '-38:59:59','-38:59:59','-38:59:59.1','-38:59:59.12','-38:59:59.123','-38:59:59.1234','-38:59:59.12345','-38:59:59.123456');
INSERT INTO date_types (__primary_key, t_time,     t_time0,    t_time1,      t_time2,       t_time3,        t_time4,         t_time5,          t_time6) VALUES (13001,
                                       '38:59:59','38:59:59','38:59:59.1','38:59:59.12','38:59:59.123','38:59:59.1234','38:59:59.12345','38:59:59.123456');
INSERT INTO date_types (__primary_key, t_time,     t_time0,    t_time1,      t_time2,       t_time3,        t_time4,         t_time5,          t_time6) VALUES (13002,
                                       '000:00:00','000:00:00','000:00:00.1','000:00:00.12','000:00:00.123','000:00:00.1234','000:00:00.12345','000:00:00.123456');

INSERT INTO date_types (__primary_key, t_datetime,           t_datetime0,          t_datetime1,            t_datetime2,             t_datetime3,              t_datetime4,               t_datetime5,                t_datetime6) VALUES (14000,
                                       '1000-01-01 00:00:00','1000-01-01 00:00:00','1000-01-01 00:00:00.1','1000-01-01 00:00:00.12','1000-01-01 00:00:00.123','1000-01-01 00:00:00.1234','1000-01-01 00:00:00.12345','1000-01-01 00:00:00.123456');
INSERT INTO date_types (__primary_key, t_datetime,           t_datetime0,          t_datetime1,            t_datetime2,             t_datetime3,              t_datetime4,               t_datetime5,                t_datetime6) VALUES (14001,
                                       '9999-12-31 23:59:59','9999-12-31 23:59:59','9999-12-31 23:59:59.1','9999-12-31 23:59:59.12','9999-12-31 23:59:59.123','9999-12-31 23:59:59.1234','9999-12-31 23:59:59.12345','9999-12-31 23:59:59.123456');
