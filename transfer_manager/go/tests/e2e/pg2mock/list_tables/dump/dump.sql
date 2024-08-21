CREATE USER blockeduser PASSWORD 'sim-sim@OPEN';

GRANT ALL PRIVILEGES ON SCHEMA public TO blockeduser;

CREATE SCHEMA extra;
REVOKE ALL PRIVILEGES ON SCHEMA extra FROM blockeduser;
GRANT USAGE ON SCHEMA extra TO blockeduser;

CREATE SCHEMA extrablocked;
REVOKE ALL PRIVILEGES ON SCHEMA extrablocked FROM blockeduser;

CREATE SCHEMA columnaccess;
REVOKE ALL PRIVILEGES ON SCHEMA columnaccess FROM blockeduser;
GRANT USAGE ON SCHEMA columnaccess TO blockeduser;


CREATE TABLE public.common(i INT, t TEXT, PRIMARY KEY (i));
INSERT INTO public.common VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TABLE public.empty(i INT, t TEXT);

CREATE VIEW public.v AS SELECT i, t FROM public.common WHERE i > 1;

CREATE MATERIALIZED VIEW public.mv AS SELECT i, t FROM public.common WHERE i < 3;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO blockeduser;


CREATE TABLE extra.common(i INT, t TEXT);
REVOKE ALL PRIVILEGES ON TABLE extra.common FROM blockeduser;
GRANT SELECT ON TABLE extra.common TO blockeduser;
INSERT INTO extra.common VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TABLE extra.empty(i INT, t TEXT, PRIMARY KEY (i));
REVOKE ALL PRIVILEGES ON TABLE extra.empty FROM blockeduser;
GRANT SELECT ON TABLE extra.empty TO blockeduser;


CREATE TABLE extrablocked.common(i INT, t TEXT PRIMARY KEY);
INSERT INTO extrablocked.common VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TABLE extrablocked.emptywithselect(i INT, t TEXT);
REVOKE ALL PRIVILEGES ON TABLE extrablocked.emptywithselect FROM blockeduser;
GRANT SELECT ON TABLE extrablocked.emptywithselect TO blockeduser;


CREATE TABLE columnaccess.table(i INT PRIMARY KEY, r TEXT, ur TEXT);
REVOKE ALL PRIVILEGES ON TABLE columnaccess.table FROM blockeduser;
GRANT SELECT(i, ur) ON TABLE columnaccess.table TO blockeduser;
