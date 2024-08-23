CREATE TYPE composite_type AS (
    pgis_geometry             GEOMETRY,
    pgis_geometry_dump        GEOMETRY_DUMP,
    pgis_geography            GEOGRAPHY,
    pgis_valid_detail         VALID_DETAIL,
    tsv                       TSVECTOR,
    pgis_geometry_array       GEOMETRY[],
    pgis_geometry_dump_array  GEOMETRY_DUMP[],
    pgis_geography_array      GEOGRAPHY[],
    pgis_valid_detail_array   VALID_DETAIL[],
    tsv_array                 TSVECTOR[]
);
