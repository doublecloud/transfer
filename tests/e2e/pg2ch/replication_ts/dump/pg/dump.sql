create table if not exists public.date_types
(
    __primary_key serial,

    t_timestamptz timestamptz null, -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a postgresql extension
    t_tst timestamp with time zone null,
    t_timetz timetz,
    t_time_with_time_zone_ time with time zone null,
    t_interval  interval null,

    t_date date null,
    t_time time null,
    t_time_1 time(1) null, -- precision: this is a fractional digits number placed in the seconds’ field. this can be up to six digits. hh:mm:ss.pppppp
    t_time_3 time(3) null,
    t_time_6 time(6) null,

    t_timetz_1 time(1) with time zone null,
    t_timetz_3 time(3) with time zone null,
    t_timetz_6 time(6) with time zone null,

    t_timestamp_1 timestamp(1) null,
    t_timestamp_3 timestamp(3) null,
    t_timestamp_6 timestamp(6) null,
    t_timestamp timestamp null
);
ALTER  TABLE public.date_types REPLICA IDENTITY FULL;

insert into public.date_types values
(
    default,
    '2004-10-19 10:23:54+02', -- TIMESTAMPTZ

    '2004-10-19 11:23:54+02', -- TIMESTAMP WITH TIME ZONE
    '00:51:02.746572-08', -- TIMETZ
    '00:51:02.746572-08', -- TIME WITH TIME ZONE
    interval '1 day 01:00:00', -- interval

    'January 8, 1999', -- date

    '04:05:06', -- time
    '04:05:06.1', -- time(1)
    '04:05:06.123', -- time(3)
    '04:05:06.123456', -- time(6)

    '2020-05-26 13:30:25.5-04', -- time(1) with time zone
    '2020-05-26 13:30:25.575-04', -- time(3) with time zone
    '2020-05-26 13:30:25.575401-04', -- time(6) with time zone

    '2004-10-19 10:23:54.9', -- timestamp(1)
    '2004-10-19 10:23:54.987', -- timestamp(3)
    '2004-10-19 10:23:54.987654', -- timestamp(6)
    '2004-10-19 10:23:54' -- timestamp
);

insert into public.date_types values (default);
