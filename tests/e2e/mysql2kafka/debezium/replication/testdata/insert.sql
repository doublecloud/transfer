INSERT INTO customers3 VALUES (
  1,

  0,     -- BOOLEAN
  1,     -- BOOL
  1,     -- BIT(1)
  X'9f', -- BIT(16)

  1,   -- TINYINT
  22,  -- TINYINT DEFAULT 0
  255, -- TINYINT UNSIGNED

  1,   -- TINYINT(1)
  1,   -- TINYINT(1) UNSIGNED

  1000, -- SMALLINT
  100,  -- SMALLINT(5)
  10,   -- SMALLINT UNSIGNED

  1,    -- MEDIUMINT
  11,   -- MEDIUMINT(5)
  111,  -- MEDIUMINT UNSIGNED

  9,     -- INT
  99,    -- INTEGER
  999,   -- INTEGER(5)
  9999,  -- INT UNSIGNED

  8,    -- BIGINT
  88,   -- BIGINT(5)
  888,  -- BIGINT UNSIGNED

  -- REAL

  123.45,   -- REAL
  99999.99, -- REAL(10, 2)

  1.23, -- FLOAT
  1.23, -- FLOAT(53)

  2.34, -- DOUBLE
  2.34, -- DOUBLE PRECISION

  -- CHAR

  'a',   -- CHAR
  'abc', -- CHAR(5)

  'blab', -- VARCHAR(5)

  X'9f', -- BINARY
  X'9f', -- BINARY(5)

  X'9f9f', -- VARBINARY(5)

  X'9f9f9f',     -- TINYBLOB
  'qwerty12345', -- TINYTEXT

  X'ff',               -- BLOB
  'my-text',           -- TEXT
  X'abcd',             -- MEDIUMBLOB
  'my-mediumtext',     -- MEDIUMTEXT
  X'abcd',             -- LONGBLOB
  'my-longtext',       -- LONGTEXT
  '{"k1": "v1"}',      -- JSON
  'x-small',           -- ENUM('x-small', 'small', 'medium', 'large', 'x-large')
  'a',                 -- SET('a', 'b', 'c', 'd')

  -- TEMPORAL DATA TYPES

  1901, -- YEAR
  2155, -- YEAR(4)

--     '1999-01-01 00:00:01',        -- TIMESTAMP    -- uncomment after TM-4377
--     '1999-10-19 10:23:54',        -- TIMESTAMP(0) -- uncomment after TM-4377
--     '2004-10-19 10:23:54.1',      -- TIMESTAMP(1) -- uncomment after TM-4377
--     '2004-10-19 10:23:54.12',     -- TIMESTAMP(2) -- uncomment after TM-4377
--     '2004-10-19 10:23:54.123',    -- TIMESTAMP(3) -- uncomment after TM-4377
--     '2004-10-19 10:23:54.1234',   -- TIMESTAMP(4) -- uncomment after TM-4377
--     '2004-10-19 10:23:54.12345',  -- TIMESTAMP(5) -- uncomment after TM-4377
--     '2004-10-19 10:23:54.123456', -- TIMESTAMP(6) -- uncomment after TM-4377

  -- TEMPORAL TYPES

  '1000-01-01',   -- DATE

  '04:05:06',        -- TIME
  '04:05:06',        -- TIME(0)
  '04:05:06.1',      -- TIME(1)
  '04:05:06.12',     -- TIME(2)
  '04:05:06.123',    -- TIME(3)
  '04:05:06.1234',   -- TIME(4)
  '04:05:06.12345',  -- TIME(5)
  '04:05:06.123456', -- TIME(6)

  '2020-01-01 18:10:10',        -- DATETIME
  '2020-01-01 18:10:10',        -- DATETIME(0)
  '2020-01-01 18:10:10.1',      -- DATETIME(1)
  '2020-01-01 18:10:10.12',     -- DATETIME(2)
  '2020-01-01 18:10:10.123',    -- DATETIME(3)
  '2020-01-01 18:10:10.1234',   -- DATETIME(4)
  '2020-01-01 18:10:10.12345',  -- DATETIME(5)
  '2020-01-01 18:10:10.123456'  -- DATETIME(6)

  -- DECIMAL TYPES

--   1234567890, -- NUMERIC -- See TM-4581
--   12345,      -- NUMERIC(5) -- See TM-4581
--   123.45,     -- NUMERIC(5,2) -- See TM-4581
--
--   2345678901, -- DECIMAL -- See TM-4581
--   23451,      -- DECIMAL(5) -- See TM-4581
--   231.45      -- DECIMAL(5,2) -- See TM-4581

  -- SPATIAL TYPES

      #     ST_GeomFromText('LINESTRING(0 0,1 2,2 4)'),                                          -- LINESTRING_         GEOMETRY,
  #     ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'),        -- POLYGON_            GEOMETRY,
  #     ST_GeomFromText('MULTIPOINT(0 0, 15 25, 45 65)'),                                    -- MULTIPOINT_         GEOMETRY,
  #     ST_GeomFromText('MULTILINESTRING((12 12, 22 22), (19 19, 32 18))'),                  -- MULTILINESTRING_    GEOMETRY,
  #     ST_GeomFromText('MULTIPOLYGON(((0 0,11 0,12 11,0 9,0 0)),((3 5,7 4,4 7,7 7,3 5)))'), -- MULTIPOLYGON_       GEOMETRY,
  #     ST_GeomFromText('GEOMETRYCOLLECTION(POINT(3 2),LINESTRING(0 0,1 3,2 5,3 5,4 7))')    -- GEOMETRYCOLLECTION_ GEOMETRY,
);
