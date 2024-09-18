CREATE TABLE customers3 (
    pk integer unsigned auto_increment,

    tinyint_    TINYINT,
    tinyint_u   TINYINT UNSIGNED,

    smallint_  SMALLINT,
    smallint_u SMALLINT UNSIGNED,

    mediumint_  MEDIUMINT,
    mediumint_u MEDIUMINT UNSIGNED,

    int_     INT,
    int_u    INT UNSIGNED,

    bigint_  BIGINT,
    bigint_u BIGINT UNSIGNED,

    --
    primary key (pk)
) engine=innodb default charset=utf8;

INSERT INTO customers3 (pk,tinyint_,tinyint_u,smallint_,smallint_u,mediumint_,mediumint_u,int_,int_u,bigint_,bigint_u) VALUES (
    1,

    -128, -- tinyint_
    0,    -- tinyint_u

    -32768, -- smallint_
    0,      -- smallint_u

    -8388608, -- mediumint_
    0,        -- mediumint_u

    -2147483648, -- int_
    0,           -- int_u

    -9223372036854775808, -- bigint_
    0                     -- bigint_u
);

INSERT INTO customers3 (pk,tinyint_,tinyint_u,smallint_,smallint_u,mediumint_,mediumint_u,int_,int_u,bigint_,bigint_u) VALUES (
    2,

    127, -- tinyint_
    255, -- tinyint_u

    32767, -- smallint_
    65535, -- smallint_u

    8388607,  -- mediumint_
    16777215, -- mediumint_u

    2147483647, -- int_
    4294967295, -- int_u

    9223372036854775807, -- bigint_
    18446744073709551615 -- bigint_u
);
