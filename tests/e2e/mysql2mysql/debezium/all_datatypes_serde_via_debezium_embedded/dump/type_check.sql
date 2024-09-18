CREATE TABLE customers3 (
    pk integer unsigned auto_increment,

    bool1 BOOLEAN,
    bool2 BOOL,
    bit   BIT(1),
    bit16  BIT(16),

    tinyint_    TINYINT,
    tinyint_def TINYINT DEFAULT 0,
    tinyint_u   TINYINT UNSIGNED,

    tinyint1    TINYINT(1),
    tinyint1u   TINYINT(1) UNSIGNED,

    smallint_  SMALLINT,
    smallint5  SMALLINT(5),
    smallint_u SMALLINT UNSIGNED,

    mediumint_  MEDIUMINT,
    mediumint5  MEDIUMINT(5),
    mediumint_u MEDIUMINT UNSIGNED,

    int_     INT,
    integer_ INTEGER,
    integer5 INTEGER(5),
    int_u    INT UNSIGNED,

    bigint_  BIGINT,
    bigint5  BIGINT(5),
    bigint_u BIGINT UNSIGNED,

    -- ---

    real_            REAL,
    real_10_2        REAL(10, 2),

    float_           FLOAT,
    float_53         FLOAT(53),

    double_          DOUBLE,
    double_precision DOUBLE PRECISION,

    -- ---

    char_            CHAR,
    char5            CHAR(5),

    varchar5         VARCHAR(5),

    binary_          BINARY,
    binary5          BINARY(5),

    varbinary5       VARBINARY(5),

    tinyblob_        TINYBLOB,
    tinytext_        TINYTEXT,

    blob_            BLOB,
    text_            TEXT,
    mediumblob_      MEDIUMBLOB,
    mediumtext_      MEDIUMTEXT,
    longblob_        LONGBLOB,
    longtext_        LONGTEXT,
    json_            JSON,
    enum_            ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
    set_             SET('a', 'b', 'c', 'd'),

    year_            YEAR,
    year4            YEAR(4),

    timestamp_       TIMESTAMP,
    timestamp0       TIMESTAMP(0),
    timestamp1       TIMESTAMP(1),
    timestamp2       TIMESTAMP(2),
    timestamp3       TIMESTAMP(3),
    timestamp4       TIMESTAMP(4),
    timestamp5       TIMESTAMP(5),
    timestamp6       TIMESTAMP(6),

    -- TEMPORAL TYPES

    date_            DATE,

    time_            TIME,
    time0            TIME(0),
--     time1            TIME(1),
    time2            TIME(2),
--     time3            TIME(3),
    time4            TIME(4),
--     time5            TIME(5),
--     time6            TIME(6),

    datetime_        DATETIME,
    datetime0        DATETIME(0),
    datetime1        DATETIME(1),
    datetime2        DATETIME(2),
    datetime3        DATETIME(3),
    datetime4        DATETIME(4),
    datetime5        DATETIME(5),
    datetime6        DATETIME(6),

    -- DECIMAL TYPES

    NUMERIC_ NUMERIC,
    NUMERIC_5 NUMERIC(5),
    NUMERIC_5_2 NUMERIC(5,2),

    DECIMAL_ DECIMAL,
    DECIMAL_5 DECIMAL(5),
    DECIMAL_5_2 DECIMAL(5,2),

    --

    primary key (pk)
) engine=innodb default charset=utf8;

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

    '1999-01-01 00:00:01',        -- TIMESTAMP
    '1999-10-19 10:23:54',        -- TIMESTAMP(0)
    '2004-10-19 10:23:54.1',      -- TIMESTAMP(1)
    '2004-10-19 10:23:54.12',     -- TIMESTAMP(2)
    '2004-10-19 10:23:54.123',    -- TIMESTAMP(3)
    '2004-10-19 10:23:54.1234',   -- TIMESTAMP(4)
    '2004-10-19 10:23:54.12345',  -- TIMESTAMP(5)
    '2004-10-19 10:23:54.123456', -- TIMESTAMP(6)

    -- TEMPORAL TYPES

    '1000-01-01',   -- DATE

    '04:05:06',        -- TIME
    '04:05:06',        -- TIME(0)
--     '04:05:06.1',      -- TIME(1)
    '04:05:06.12',     -- TIME(2)
--     '04:05:06.123',    -- TIME(3)
    '04:05:06.1234',   -- TIME(4)
--     '04:05:06.12345',  -- TIME(5)
--     '04:05:06.123456', -- TIME(6)

    '2020-01-01 15:10:10',        -- DATETIME
    '2020-01-01 15:10:10',        -- DATETIME(0)
    '2020-01-01 15:10:10.1',      -- DATETIME(1)
    '2020-01-01 15:10:10.12',     -- DATETIME(2)
    '2020-01-01 15:10:10.123',    -- DATETIME(3)
    '2020-01-01 15:10:10.1234',   -- DATETIME(4)
    '2020-01-01 15:10:10.12345',  -- DATETIME(5)
    '2020-01-01 15:10:10.123456', -- DATETIME(6)

    -- DECIMAL TYPES

    1234567890, -- NUMERIC
    12345,      -- NUMERIC(5)
    123.45,     -- NUMERIC(5,2)

    2345678901, -- DECIMAL
    23451,      -- DECIMAL(5)
    231.45      -- DECIMAL(5,2)
);
