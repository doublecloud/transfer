CREATE TABLE test (
    i BIGSERIAL PRIMARY KEY,
    j JSON,
    jb JSONB
);

CREATE TABLE test_not_transformed (
    i BIGSERIAL PRIMARY KEY,
    j JSON,
    jb JSONB
);

INSERT INTO test(j, jb) VALUES (
    '{"key": 100.01}', -- j
    '{"key": 100.01}'  -- jb
);

INSERT INTO test_not_transformed(j, jb) VALUES (
    '{"key": 100.01}', -- j
    '{"key": 100.01}'  -- jb
);

UPDATE test SET j = '{"key": 123.45}', jb = '{"key": 123.45}' WHERE i = 1;

UPDATE test_not_transformed SET j = '{"key": 123.45}', jb = '{"key": 123.45}' WHERE i = 1;
