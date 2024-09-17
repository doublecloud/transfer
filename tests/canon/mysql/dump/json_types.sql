create table if not exists json_types
(
    __primary_key INT PRIMARY KEY,

    --

    t_json JSON
);

INSERT INTO json_types (__primary_key, t_json) VALUES (0, '{"a": "b"}');
INSERT INTO json_types (__primary_key, t_json) VALUES (1, '["a", "b"]');
INSERT INTO json_types (__primary_key, t_json) VALUES (2, '"blablabla"');
INSERT INTO json_types (__primary_key, t_json) VALUES (3, '1');
INSERT INTO json_types (__primary_key, t_json) VALUES (4, '{}');
INSERT INTO json_types (__primary_key, t_json) VALUES (5, '[]');

-- null case
INSERT INTO json_types (__primary_key) VALUES (801640048);
