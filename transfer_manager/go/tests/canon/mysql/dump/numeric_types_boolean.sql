create table if not exists numeric_types_boolean
(
    __primary_key INT PRIMARY KEY,

    -- bool

    t_boolean BOOLEAN,
    t_bool    BOOL
);


INSERT INTO numeric_types_boolean (__primary_key, t_boolean, t_bool) VALUES (10000, false, false);
INSERT INTO numeric_types_boolean (__primary_key, t_boolean, t_bool) VALUES (10001, true, true);

-- null case
INSERT INTO numeric_types_boolean (__primary_key) VALUES (801640048);
