CREATE TABLE `test` (
    id      INTEGER PRIMARY KEY,
    f       FLOAT,
    f10     FLOAT(10),
    f22     FLOAT(20),
    f24     FLOAT(22),
    f30     FLOAT(30),
    f10_5   FLOAT(10,5),
    f20_10  FLOAT(20,10),
    f10_2   FLOAT(10,2)
);

-- epsilon for float32: 1.1920929e-07
-- epsilon for float64: 2.220446049250313e-16

INSERT  INTO  `test`  (id,  f)       VALUES  (1,   0);
INSERT  INTO  `test`  (id,  f)       VALUES  (2,   1.1920929e-07);
INSERT  INTO  `test`  (id,  f)       VALUES  (3,   1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f)       VALUES  (4,   1.19209291e-07);
INSERT  INTO  `test`  (id,  f)       VALUES  (5,   2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f)       VALUES  (6,   1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f)       VALUES  (7,   2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f10)     VALUES  (8,   0);
INSERT  INTO  `test`  (id,  f10)     VALUES  (9,   1.1920929e-07);
INSERT  INTO  `test`  (id,  f10)     VALUES  (10,  1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f10)     VALUES  (11,  1.19209291e-07);
INSERT  INTO  `test`  (id,  f10)     VALUES  (12,  2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f10)     VALUES  (13,  1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f10)     VALUES  (14,  2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f22)     VALUES  (15,  0);
INSERT  INTO  `test`  (id,  f22)     VALUES  (16,  1.1920929e-07);
INSERT  INTO  `test`  (id,  f22)     VALUES  (17,  1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f22)     VALUES  (18,  1.19209291e-07);
INSERT  INTO  `test`  (id,  f22)     VALUES  (19,  2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f22)     VALUES  (20,  1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f22)     VALUES  (21,  2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f24)     VALUES  (22,  0);
INSERT  INTO  `test`  (id,  f24)     VALUES  (23,  1.1920929e-07);
INSERT  INTO  `test`  (id,  f24)     VALUES  (24,  1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f24)     VALUES  (25,  1.19209291e-07);
INSERT  INTO  `test`  (id,  f24)     VALUES  (26,  2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f24)     VALUES  (27,  1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f24)     VALUES  (28,  2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f30)     VALUES  (29,  0);
INSERT  INTO  `test`  (id,  f30)     VALUES  (30,  1.1920929e-07);
INSERT  INTO  `test`  (id,  f30)     VALUES  (31,  1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f30)     VALUES  (32,  1.19209291e-07);
INSERT  INTO  `test`  (id,  f30)     VALUES  (33,  2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f30)     VALUES  (34,  1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f30)     VALUES  (35,  2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (36,  0);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (37,  1.1920929e-07);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (38,  1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (39,  1.19209291e-07);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (40,  2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (41,  1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (42,  2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (43,  12345.12345);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (44,  1234567890.12345);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (45,  1234567890.0123456789);
INSERT  INTO  `test`  (id,  f10_5)   VALUES  (46,  12345678901234567890.01234567890123456789);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (47,  0);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (48,  1.1920929e-07);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (49,  1+1.1920929e-07);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (50,  1.19209291e-07);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (51,  2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (52,  1+2.220446049250313e-16);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (53,  2.2204460492503131e-16);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (54,  12345.12345);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (55,  1234567890.12345);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (56,  1234567890.0123456789);
INSERT  INTO  `test`  (id,  f20_10)  VALUES  (57,  12345678901234567890.01234567890123456789);
INSERT  INTO  `test`  (id,  f10_2)   VALUES  (58,  1.23);
