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
    time1            TIME(1),
    time2            TIME(2),
    time3            TIME(3),
    time4            TIME(4),
    time5            TIME(5),
    time6            TIME(6),

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

    -- SPATIAL TYPES

    --     LINESTRING_         GEOMETRY,
    --     POLYGON_            GEOMETRY,
    --     MULTIPOINT_         GEOMETRY,
    --     MULTILINESTRING_    GEOMETRY,
    --     MULTIPOLYGON_       GEOMETRY,
    --     GEOMETRYCOLLECTION_ GEOMETRY,

    --

    primary key (pk)
) engine=innodb default charset=utf8;
