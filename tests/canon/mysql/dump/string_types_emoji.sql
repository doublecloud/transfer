create table if not exists string_types_emoji
(
    __primary_key INT PRIMARY KEY,

    --

    t_char           CHAR
);

ALTER TABLE string_types_emoji CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO string_types_emoji (__primary_key,t_char) VALUES (10000,'❌');

-- null case
INSERT INTO string_types_emoji (__primary_key) VALUES (801640048);
