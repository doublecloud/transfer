CREATE TYPE my_enum_type AS ENUM (
    'VALUE_ONE',
    'VALUE_TWO',
    'VALUE_THREE'
    );

CREATE TABLE public.user_table
(
    i int PRIMARY KEY,
    d double precision,
    enum_arr my_enum_type[]

);

INSERT INTO public.user_table VALUES (1, 0,ARRAY []::my_enum_type[]);
INSERT INTO public.user_table VALUES (2, 'Infinity',ARRAY ['VALUE_ONE']::my_enum_type[]);
INSERT INTO public.user_table VALUES (3, 'NaN',ARRAY ['VALUE_TWO','VALUE_THREE']::my_enum_type[]);
INSERT INTO public.user_table VALUES (4, '-Infinity', ARRAY ['VALUE_TWO']::my_enum_type[]);

