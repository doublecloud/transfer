CREATE USER loser WITH PASSWORD '123';

CREATE TABLE public.promiscuous (
    id      integer     PRIMARY KEY,
    value   text
);
INSERT INTO promiscuous VALUES (1, '1'), (2, '2'), (3, '3');

CREATE TABLE public.secret (
    id      integer     PRIMARY KEY,
    value   text
);

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM public, loser;
GRANT ALL PRIVILEGES ON TABLE public.promiscuous TO loser;
ALTER ROLE loser WITH REPLICATION;

INSERT INTO public.secret VALUES (11, '11'), (22, '22'), (33, '33');
