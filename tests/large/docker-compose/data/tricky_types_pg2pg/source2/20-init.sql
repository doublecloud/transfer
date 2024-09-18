CREATE TABLE pgis_supported_types (
    id                  INTEGER PRIMARY KEY,
    pgis_geometry       GEOMETRY,
    pgis_geometry_dump  GEOMETRY_DUMP,
    pgis_geography      GEOGRAPHY,
    pgis_valid_detail   VALID_DETAIL
);

INSERT INTO
    pgis_supported_types(id, pgis_geometry)
VALUES
    (1, 'POINT(46.285622 48.003578 0.000000)'),
    (2, 'POINT(46.285831 48.003696 0.000000)'),
    (3, 'POINT(46.285892 48.00399 0.000000)')
;
