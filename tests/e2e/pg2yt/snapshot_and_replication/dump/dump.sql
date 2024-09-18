CREATE TABLE public.table_simple(id INT PRIMARY KEY, val TEXT);
INSERT INTO public.table_simple VALUES (1, '111');

CREATE TABLE public.table_simple__replica_identity_full(id INT, val TEXT);
ALTER  TABLE public.table_simple__replica_identity_full REPLICA IDENTITY FULL;
INSERT INTO public.table_simple__replica_identity_full VALUES (1, '111');
