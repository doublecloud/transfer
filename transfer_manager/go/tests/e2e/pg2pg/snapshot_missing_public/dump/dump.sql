CREATE SCHEMA mysch;

ALTER DATABASE postgres SET search_path = 'mysch';

ALTER ROLE postgres SET search_path = 'mysch';

CREATE TABLE mysch.t(i INT PRIMARY KEY, t TEXT);
INSERT INTO mysch.t(i, t) VALUES (1, 'a'), (2, 'b');

CREATE TABLE public.t2(i INT PRIMARY KEY, f REAL);
INSERT INTO public.t2(i, f) VALUES (1, 1.0), (2, 4.0);
