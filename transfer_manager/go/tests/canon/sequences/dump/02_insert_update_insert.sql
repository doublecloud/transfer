INSERT INTO insert_update_delete(i1, i2, t) VALUES (1, 1, '1a');
UPDATE insert_update_delete SET t = '1b' WHERE i1 = 1 AND i2 = 1;
UPDATE insert_update_delete SET i1 = 2, i2 = 2 WHERE i1 = 1 AND i2 = 1;

INSERT INTO insert_update_delete(i1, i2, t) VALUES (1, 1, '2a');
UPDATE insert_update_delete SET t = '2b' WHERE i1 = 1 AND i2 = 1;

INSERT INTO insert_update_delete(i1, i2, t) VALUES (3, 3, '3a');
UPDATE insert_update_delete SET t = '3b' WHERE i1 = 3 AND i2 = 3;
DELETE FROM insert_update_delete WHERE i1 = 3 AND i2 = 3;
INSERT INTO insert_update_delete(i1, i2, t) VALUES (3, 3, '4a');
UPDATE insert_update_delete SET t = '4b' WHERE i1 = 3 AND i2 = 3;
