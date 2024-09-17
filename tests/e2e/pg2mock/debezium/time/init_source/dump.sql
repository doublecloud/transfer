CREATE TABLE basic_types (
    id INT PRIMARY KEY,
    t_timestamp_without_tz timestamp without time zone,
    t_timestamp_with_tz    timestamp with time zone,
    t_time_without_tz      time without time zone,
    t_time_with_tz         time with time zone,
    t_interval             interval
);

INSERT INTO basic_types (id, t_timestamp_without_tz, t_timestamp_with_tz, t_time_without_tz, t_time_with_tz, t_interval) VALUES (
    1,
    '2022-08-28 19:49:47.749906',
    '2022-08-28 19:49:47.749906 +00:00',
    '19:49:47.749906',
    '19:49:47.749906 +00:00',
    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 7 microseconds'
);

INSERT INTO basic_types (id, t_timestamp_without_tz, t_timestamp_with_tz, t_time_without_tz, t_time_with_tz, t_interval) VALUES (
    2,
    '2022-08-28 19:49:47.74990',
    '2022-08-28 19:49:47.74990 +00:00',
    '19:49:47.74990',
    '19:49:47.74990 +00:00',
    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 70 microseconds'
);

-- ---

CREATE TABLE table_with_timestamp (
    id INT PRIMARY KEY,
    t_timestamp timestamp
);

INSERT INTO table_with_timestamp (id, t_timestamp) VALUES (
    1,
    '1900-01-01 03:00:00 +0230'
);
