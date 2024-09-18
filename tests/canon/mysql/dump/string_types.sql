create table if not exists string_types
(
    __primary_key INT PRIMARY KEY,

    --

    t_char           CHAR,
    t_char5          CHAR(5),

    t_varchar5       VARCHAR(5),

    t_binary         BINARY,
    t_binary5        BINARY(5),

    t_varbinary5     VARBINARY(5),

    t_tinyblob       TINYBLOB,
    t_tinytext       TINYTEXT,
    t_blob           BLOB,
    t_text           TEXT,
    t_mediumblob     MEDIUMBLOB,
    t_mediumtext     MEDIUMTEXT,
    t_longblob       LONGBLOB,
    t_longtext       LONGTEXT,
    t_enum           ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
    t_set            SET('a', 'b', 'c', 'd')
);

INSERT INTO string_types (__primary_key,t_char) VALUES (10000,'a');

INSERT INTO string_types (__primary_key,t_char5) VALUES (11000,'a');
INSERT INTO string_types (__primary_key,t_char5) VALUES (11001,'abcde');

INSERT INTO string_types (__primary_key,t_varchar5) VALUES (12000,'a');
INSERT INTO string_types (__primary_key,t_varchar5) VALUES (12001,'abcde');

INSERT INTO string_types (__primary_key,t_binary) VALUES (13000,X'9f');
INSERT INTO string_types (__primary_key,t_binary) VALUES (13001,X'fd');

INSERT INTO string_types (__primary_key,t_binary5) VALUES (14000,X'9f');
INSERT INTO string_types (__primary_key,t_binary5) VALUES (14001,X'fd');
INSERT INTO string_types (__primary_key,t_binary5) VALUES (14002,X'9f9f9f9f9f');
INSERT INTO string_types (__primary_key,t_binary5) VALUES (14003,X'fdfdfdfdfd');

INSERT INTO string_types (__primary_key,t_varbinary5) VALUES (15000,X'9f');
INSERT INTO string_types (__primary_key,t_varbinary5) VALUES (15001,X'fd');
INSERT INTO string_types (__primary_key,t_varbinary5) VALUES (15002,X'9f9f9f9f9f');
INSERT INTO string_types (__primary_key,t_varbinary5) VALUES (15003,X'fdfdfdfdfd');

INSERT INTO string_types (__primary_key,t_tinyblob) VALUES (16000,X'9f');
INSERT INTO string_types (__primary_key,t_tinyblob) VALUES (16001,X'fd');
INSERT INTO string_types (__primary_key,t_tinytext) VALUES (16002,'blablabla');

INSERT INTO string_types (__primary_key,t_blob) VALUES (17000,X'9f');
INSERT INTO string_types (__primary_key,t_blob) VALUES (17001,X'fd');
INSERT INTO string_types (__primary_key,t_blob) VALUES (17002,X'9f9f9f9f9f');
INSERT INTO string_types (__primary_key,t_blob) VALUES (17003,X'fdfdfdfdfd');

INSERT INTO string_types (__primary_key,t_text) VALUES (18000,'blablabla');

INSERT INTO string_types (__primary_key,t_mediumblob) VALUES (19000,X'9f');
INSERT INTO string_types (__primary_key,t_mediumblob) VALUES (19001,X'fd');
INSERT INTO string_types (__primary_key,t_mediumblob) VALUES (19002,X'9f9f9f9f9f');
INSERT INTO string_types (__primary_key,t_mediumblob) VALUES (19003,X'fdfdfdfdfd');

INSERT INTO string_types (__primary_key,t_mediumtext) VALUES (20000,'blablabla');

INSERT INTO string_types (__primary_key,t_longblob) VALUES (21000,X'9f');
INSERT INTO string_types (__primary_key,t_longblob) VALUES (21001,X'fd');
INSERT INTO string_types (__primary_key,t_longblob) VALUES (21002,X'9f9f9f9f9f');
INSERT INTO string_types (__primary_key,t_longblob) VALUES (21003,X'fdfdfdfdfd');

INSERT INTO string_types (__primary_key,t_longtext) VALUES (22000,'blablabla');

INSERT INTO string_types (__primary_key,t_enum) VALUES (23000,'x-large');

INSERT INTO string_types (__primary_key,t_set) VALUES (24000,'c');

-- null case
INSERT INTO string_types (__primary_key) VALUES (801640048);
