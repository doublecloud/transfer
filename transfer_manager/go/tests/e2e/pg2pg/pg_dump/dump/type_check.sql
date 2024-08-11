--
-- Name: __english_collation; Type: COLLATION; Schema: public; Owner: -
--

CREATE COLLATION __english_collation (provider = libc, locale = 'en_US.UTF-8');

--
-- Name: __test; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE __test (
    id   integer,
    name character varying(255)
);

--
-- Name: __name_changes(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION __name_changes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.name <> OLD.name THEN
         INSERT INTO __test(id,name)
         VALUES(OLD.id,OLD.name);
    END IF;

    RETURN NEW;
END;
$$;

--
-- Name: __test __name_changes_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER __name_changes_trigger BEFORE UPDATE ON __test FOR EACH ROW EXECUTE PROCEDURE __name_changes();

--
-- Name: __test __test_policy; Type: POLICY; Schema: public; Owner: db_user
--

CREATE POLICY __test_policy ON __test USING (((name)::text = 'test'::text));

CREATE VIEW __test_view AS SELECT id, name FROM __test WHERE id > 0;

CREATE MATERIALIZED VIEW __test_materialized_view AS SELECT id, name FROM __test WHERE id < 0;

CREATE SCHEMA santa;

CREATE TYPE santa.my_enum AS ENUM ('RED', 'BLUE');

CREATE TABLE santa."Ho-Ho-Ho"(i SERIAL PRIMARY KEY, t TEXT, f FLOAT, j santa.my_enum);

INSERT INTO santa."Ho-Ho-Ho"(t, f, j) VALUES ('merry', 1.0, 'BLUE'), ('Christmas', 2.0, 'RED');

CREATE SEQUENCE santa."Rudolf" START 2;


CREATE TABLE wide_boys(column_1 int, column_2 int) PARTITION BY RANGE (column_2);

-- first type of part attachment
CREATE TABLE wide_boys_part_1 PARTITION OF wide_boys FOR VALUES FROM (0) TO (10);

-- second type of part attachment
CREATE TABLE wide_boys_part_2 (column_1 int primary key, column_2 int);
ALTER TABLE wide_boys ATTACH PARTITION wide_boys_part_2 FOR VALUES FROM (10) TO (25);

