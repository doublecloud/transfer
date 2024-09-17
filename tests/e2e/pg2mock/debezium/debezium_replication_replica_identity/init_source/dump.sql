CREATE TABLE public.basic_types
(
    id INT PRIMARY KEY,
    val text
);
ALTER  TABLE public.basic_types REPLICA IDENTITY FULL;
INSERT INTO public.basic_types (id, val) VALUES (1, 'blablabla');
