create user conn_test WITH REPLICATION LOGIN ENCRYPTED password 'aA_12345' connection limit 6;
create user writer password 'aA_12345';


CREATE TABLE public.test1(
    id         SERIAL             NOT NULL PRIMARY KEY,
    value      INT
);

CREATE TABLE public.test2(
                      id         SERIAL             NOT NULL PRIMARY KEY,
                      value      INT
);

CREATE TABLE public.test3(
                      id         SERIAL             NOT NULL PRIMARY KEY,
                      value      INT
);

CREATE TABLE public.test4(
                      id         SERIAL             NOT NULL PRIMARY KEY,
                      value      INT
);

CREATE TABLE public.test5(
                      id         SERIAL             NOT NULL PRIMARY KEY,
                      value      INT
);

CREATE TABLE public.test6(
                      id         SERIAL             NOT NULL PRIMARY KEY,
                      value      INT
);

INSERT INTO public.test1(value) SELECT generate_series(1, 1000000);
INSERT INTO public.test2(value) SELECT generate_series(1, 1000000);
INSERT INTO public.test3(value) SELECT generate_series(1, 1000000);
INSERT INTO public.test4(value) SELECT generate_series(1, 1000000);
INSERT INTO public.test5(value) SELECT generate_series(1, 1000000);
INSERT INTO public.test6(value) SELECT generate_series(1, 1000000);

GRANT ALL PRIVILEGES ON SCHEMA public TO conn_test;
GRANT ALL PRIVILEGES ON SCHEMA public TO writer;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO conn_test;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO writer;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO conn_test;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO writer;


