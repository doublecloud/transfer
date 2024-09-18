CREATE TABLE temporals (id INTEGER, d DATE, t TIME, ts TIMESTAMP WITHOUT TIME ZONE, tstz TIMESTAMP WITH TIME ZONE, PRIMARY KEY (id, d));

INSERT INTO temporals VALUES
(1, '-infinity', 'allballs', '-infinity', '-infinity'),
(2, 'infinity', 'allballs', 'infinity', 'infinity'),
(3, '1999-12-31', NULL, NULL, NULL);
