create table if not exists numeric_types_bit
(
    __primary_key INT PRIMARY KEY,

    -- bit
    -- https://dev.mysql.com/doc/refman/8.0/en/bit-type.html
    -- A type of BIT(M) enables storage of M-bit values. M can range from 1 to 64.

    t_bit   BIT,
    t_bit1  BIT(1),
    t_bit16 BIT(16),
    t_bit64 BIT(64)
);


INSERT INTO numeric_types_bit (__primary_key, t_bit, t_bit1, t_bit16, t_bit64) VALUES (11000, b'0', b'0', b'0000000000000000', b'0000000000000000000000000000000000000000000000000000000000000000');
INSERT INTO numeric_types_bit (__primary_key, t_bit, t_bit1, t_bit16, t_bit64) VALUES (11001, b'1', b'1', b'1111111111111111', b'1111111111111111111111111111111111111111111111111111111111111111');

-- null case
INSERT INTO numeric_types_bit (__primary_key) VALUES (801640048);
