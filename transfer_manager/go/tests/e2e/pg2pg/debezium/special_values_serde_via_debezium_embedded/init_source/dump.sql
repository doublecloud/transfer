create table if not exists public.my_table
(
    i int PRIMARY KEY,

    t_smallint smallint,
    t_integer integer,
    t_bigint bigint,

    t_numeric_18_2 numeric(18,2),

    TIMESTAMPTZ_ TIMESTAMPTZ
);

INSERT INTO public.my_table VALUES
(
    0,

    -32768, -- t_smallint
    -2147483648, -- t_integer
    -9223372036854775808,

    -0.01,

    '2022-08-28 19:49:47.090000Z' -- TIMESTAMPTZ
);

INSERT INTO public.my_table VALUES
(
    1,

    32767, -- t_smallint
    2147483647, -- t_integer
    9223372036854775807, -- t_bigint

    0.01,

    '2022-08-28 19:49:47.090000Z' -- TIMESTAMPTZ
);
