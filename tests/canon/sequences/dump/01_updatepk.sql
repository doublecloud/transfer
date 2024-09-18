INSERT INTO insert_update_delete(i1, i2, t) VALUES (1, 1, 'a');
UPDATE insert_update_delete SET t = 'b' WHERE i1 = 1 AND i2 = 1;
UPDATE insert_update_delete SET i1 = 2, i2 = 2 WHERE i1 = 1 AND i2 = 1;
UPDATE insert_update_delete SET t = 'c' WHERE i1 = 2 AND i2 = 2;
