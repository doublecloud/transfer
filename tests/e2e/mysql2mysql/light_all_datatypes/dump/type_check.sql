CREATE TABLE `__test` (
                                              -- If you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to the column.
    `tinyint`     TINYINT,
    `tinyint_def` TINYINT DEFAULT 0,
    `tinyint_u`   TINYINT UNSIGNED,
    `tinyint_z`   TINYINT ZEROFILL,
    `smallint`    SMALLINT,
    `smallint_u`  SMALLINT UNSIGNED,
    `smallint_z`  SMALLINT ZEROFILL,
    `mediumint`   MEDIUMINT,
    `mediumint_u` MEDIUMINT UNSIGNED,
    `mediumint_z` MEDIUMINT ZEROFILL,
    `int`         INT,
    `int_u`       INT UNSIGNED,
    `int_z`       INT ZEROFILL,
    `bigint`      BIGINT,
    `bigint_u`    BIGINT UNSIGNED,
    `bigint_z`    BIGINT ZEROFILL,

    `bool` BOOL,                              -- synonym to TINYINT(1)

    `decimal_10_2` DECIMAL(10, 2),            -- synonyms: decimal, dec, numeric, fixed
    `decimal_65_30` DECIMAL(65, 30),
    `decimal_65_0` DECIMAL(65, 0),
    `dec`     DEC,
    `numeric` NUMERIC(11, 3),
    `fixed`   FIXED,

                                              -- "As of MySQL 8.0.17, the UNSIGNED attribute is deprecated for columns of type FLOAT, DOUBLE, and DECIMAL (and any synonyms); you should expect support for it to be removed in a future version of MySQL."
    `float`            FLOAT(10, 2),          -- "As of MySQL 8.0.17, the nonstandard FLOAT(M,D) and DOUBLE(M,D) syntax is deprecated and you should expect support for it to be removed in a future version of MySQL."
    `float_z`          FLOAT(10, 2) ZEROFILL, -- same
    `float_53`         FLOAT(53),             -- same
    `real`             REAL(10, 2),           -- same && synonym to FLOAT
    `double`           DOUBLE,
    `double_precision` DOUBLE PRECISION,

    `bit`    BIT,
    `bit_5`  BIT(5),
    `bit_9`  BIT(9),
    `bit_64` BIT(64),

    `date`        DATE,
    `datetime`    DATETIME,
    `datetime_6`  DATETIME(6),
    `timestamp`   TIMESTAMP,
    `timestamp_2` TIMESTAMP(2),

    `time`   TIME,
    `time_2` TIME(2),
    `year`   YEAR,

    `char`        CHAR(10),
    `varchar`     VARCHAR(20),
    `varchar_def` VARCHAR(20) DEFAULT 'default_value',

    `binary`    BINARY(20),
    `varbinary` VARBINARY(20),

    `tinyblob`   TINYBLOB,
    `blob`       BLOB,
    `mediumblob` MEDIUMBLOB,
    `longblob`   LONGBLOB,

    `tinytext` TINYTEXT  ,
    `text` TEXT,
    `mediumtext` MEDIUMTEXT  ,
    `longtext` LONGTEXT  ,

    `enum` ENUM('1', '2', '3'),
    `set`  SET ('1', '2', '3'),

    -- json

    `json` JSON,


    `id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY         -- just to have a primary key
) engine=innodb default charset=utf8;
