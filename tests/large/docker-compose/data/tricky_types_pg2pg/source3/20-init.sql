CREATE TABLE pgis_box3d_unsupported  (id INTEGER PRIMARY KEY, pgis_box3d BOX3D);
CREATE TABLE pgis_box2d_unsupported  (id INTEGER PRIMARY KEY, pgis_box3d BOX2D);

INSERT INTO pgis_box3d_unsupported  VALUES (1, ST_GeomFromEWKT('LINESTRING(1 2 3, 3 4 5, 5 6 5)'));
INSERT INTO pgis_box2d_unsupported  VALUES (1, ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'));
