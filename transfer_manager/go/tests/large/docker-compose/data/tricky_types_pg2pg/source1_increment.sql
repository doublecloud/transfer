INSERT INTO
    pgis_supported_types(
        id,
        pgis_geometry,
        pgis_geometry_dump,
        pgis_geography,
        pgis_valid_detail,
        tsv,
        pgis_geometry_array,
        pgis_geometry_dump_array,
        pgis_geography_array,
        pgis_valid_detail_array,
        tsv_array,
        composite,
        composite_array
    )
VALUES
    (
        11,
        'POINT(46.285622 48.003578 0.000000)',
        (ARRAY[]::INT[], 'POINT(46.285622 48.003578 0.000000)'),
        'POINT(46.285622 48.003578 0.000000)',
        ST_IsValidDetail(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 2 1, 2 2, 1 2, 1 1, 1 0, 0 0))')),
        'a fat cat sat on a mat and ate a fat rat',
        ARRAY['POINT(46.285622 48.003578 0.000000)'::GEOMETRY],
        ARRAY[(ARRAY[]::INT[], 'POINT(46.285622 48.003578 0.000000)')::GEOMETRY_DUMP],
        ARRAY['POINT(46.285622 48.003578 0.000000)'::GEOGRAPHY],
        ARRAY[ST_IsValidDetail(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 2 1, 2 2, 1 2, 1 1, 1 0, 0 0))'))::VALID_DETAIL],
        ARRAY['a fat cat sat on a mat and ate a fat rat'::TSVECTOR],
        (
            'POINT(46.285622 48.003578 0.000000)'::GEOMETRY,
            (ARRAY[]::INT[], 'POINT(46.285622 48.003578 0.000000)')::GEOMETRY_DUMP,
            'POINT(46.285622 48.003578 0.000000)'::GEOGRAPHY,
            ST_IsValidDetail(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 2 1, 2 2, 1 2, 1 1, 1 0, 0 0))'))::VALID_DETAIL,
            'a fat cat sat on a mat and ate a fat rat',
            ARRAY['POINT(46.285622 48.003578 0.000000)'::GEOMETRY]::GEOMETRY[],
            ARRAY[(ARRAY[]::INT[], 'POINT(46.285622 48.003578 0.000000)')::GEOMETRY_DUMP]::GEOMETRY_DUMP[],
            ARRAY['POINT(46.285622 48.003578 0.000000)'::GEOGRAPHY]::GEOGRAPHY[],
            ARRAY[ST_IsValidDetail(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 2 1, 2 2, 1 2, 1 1, 1 0, 0 0))'))::VALID_DETAIL]::VALID_DETAIL[],
            ARRAY['a fat cat sat on a mat and ate a fat rat'::TSVECTOR]
        ),
        ARRAY[(
            'POINT(46.285622 48.003578 0.000000)'::GEOMETRY,
            (ARRAY[]::INT[], 'POINT(46.285622 48.003578 0.000000)')::GEOMETRY_DUMP,
            'POINT(46.285622 48.003578 0.000000)'::GEOGRAPHY,
            ST_IsValidDetail(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 2 1, 2 2, 1 2, 1 1, 1 0, 0 0))'))::VALID_DETAIL,
            'a fat cat sat on a mat and ate a fat rat',
            ARRAY['POINT(46.285622 48.003578 0.000000)'::GEOMETRY]::GEOMETRY[],
            ARRAY[(ARRAY[]::INT[], 'POINT(46.285622 48.003578 0.000000)')::GEOMETRY_DUMP]::GEOMETRY_DUMP[],
            ARRAY['POINT(46.285622 48.003578 0.000000)'::GEOGRAPHY]::GEOGRAPHY[],
            ARRAY[ST_IsValidDetail(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 2 1, 2 2, 1 2, 1 1, 1 0, 0 0))'))::VALID_DETAIL]::VALID_DETAIL[],
            ARRAY['a fat cat sat on a mat and ate a fat rat'::TSVECTOR]
        )::COMPOSITE_TYPE]
    ),
    (
        22,
        'POINT(46.285831 48.003696 0.000000)',
        (ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)'),
        'POINT(46.285831 48.003696 0.000000)',
        ST_IsValidDetail(ST_GeometryFromText('POINT(46.285831 48.003696 0.000000)')),
        'oh my god they killed kenny',
        ARRAY['POINT(46.285831 48.003696 0.000000)'::GEOMETRY],
        ARRAY[(ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)')::GEOMETRY_DUMP],
        ARRAY['POINT(46.285831 48.003696 0.000000)'::GEOGRAPHY],
        ARRAY[ST_IsValidDetail(ST_GeometryFromText('POINT(46.285831 48.003696 0.000000)'))::VALID_DETAIL],
        ARRAY['oh my god they killed kenny'::TSVECTOR],
        (
            'POINT(46.285831 48.003696 0.000000)'::GEOMETRY,
            (ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)')::GEOMETRY_DUMP,
            'POINT(46.285831 48.003696 0.000000)'::GEOGRAPHY,
            ST_IsValidDetail(ST_GeometryFromText('POINT(46.285831 48.003696 0.000000)'))::VALID_DETAIL,
            'oh my god they killed kenny',
            ARRAY['POINT(46.285831 48.003696 0.000000)'::GEOMETRY]::GEOMETRY[],
            ARRAY[(ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)')::GEOMETRY_DUMP]::GEOMETRY_DUMP[],
            ARRAY['POINT(46.285831 48.003696 0.000000)'::GEOGRAPHY]::GEOGRAPHY[],
            ARRAY[ST_IsValidDetail(ST_GeometryFromText('POINT(46.285831 48.003696 0.000000)'))::VALID_DETAIL]::VALID_DETAIL[],
            ARRAY['oh my god they killed kenny'::TSVECTOR]
        ),
        ARRAY[(
            'POINT(46.285831 48.003696 0.000000)'::GEOMETRY,
            (ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)')::GEOMETRY_DUMP,
            'POINT(46.285831 48.003696 0.000000)'::GEOGRAPHY,
            ST_IsValidDetail(ST_GeometryFromText('POINT(46.285831 48.003696 0.000000)'))::VALID_DETAIL,
            'oh my god they killed kenny',
            ARRAY['POINT(46.285831 48.003696 0.000000)'::GEOMETRY]::GEOMETRY[],
            ARRAY[(ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)')::GEOMETRY_DUMP]::GEOMETRY_DUMP[],
            ARRAY['POINT(46.285831 48.003696 0.000000)'::GEOGRAPHY]::GEOGRAPHY[],
            ARRAY[ST_IsValidDetail(ST_GeometryFromText('POINT(46.285831 48.003696 0.000000)'))::VALID_DETAIL]::VALID_DETAIL[],
            ARRAY['oh my god they killed kenny'::TSVECTOR]
        )::COMPOSITE_TYPE]
    ),
    (
        33,
        'POINT(46.285892 48.00399 0.000000)',
        (ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)'),
        'POINT(46.285892 48.00399 0.000000)',
        NULL,
        NULL,
        NULL,
        ARRAY[]::GEOMETRY_DUMP[],
        ARRAY['POINT(46.285892 48.00399 0.000000)'::GEOGRAPHY],
        ARRAY[NULL::VALID_DETAIL],
        ARRAY[NULL::TSVECTOR],
        (
            'POINT(46.285892 48.00399 0.000000)'::GEOMETRY,
            (ARRAY[]::INT[], 'POINT(46.285831 48.003696 0.000000)')::GEOMETRY_DUMP,
            'POINT(46.285892 48.00399 0.000000)'::GEOGRAPHY,
            NULL::VALID_DETAIL,
            NULL::TSVECTOR,
            NULL::GEOMETRY[],
            ARRAY[]::GEOMETRY_DUMP[],
            ARRAY['POINT(46.285892 48.00399 0.000000)'::GEOGRAPHY]::GEOGRAPHY[],
            ARRAY[NULL::VALID_DETAIL]::VALID_DETAIL[],
            ARRAY[NULL::TSVECTOR]
        ),
        ARRAY[NULL::COMPOSITE_TYPE]
    ),
    (
        44,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    )
;
