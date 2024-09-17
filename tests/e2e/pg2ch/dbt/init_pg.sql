DROP TABLE IF EXISTS public.t1;
CREATE TABLE public.t1(i INT PRIMARY KEY, t TEXT);
INSERT INTO public.t1(i, t) SELECT gs, gs::TEXT FROM generate_series(1, 100) AS gs;

DROP TABLE IF EXISTS public.t2;
CREATE TABLE public.t2(i INT PRIMARY KEY, f FLOAT);
INSERT INTO public.t2(i, f) SELECT gs, gs::FLOAT FROM generate_series(5, 104) AS gs;

DROP TABLE IF EXISTS public.t3;
CREATE TABLE public.t3(t TEXT PRIMARY KEY, i INT UNIQUE NOT NULL);
INSERT INTO public.t3(t, i) SELECT gs::TEXT, gs FROM generate_series(10, 109) AS gs;
