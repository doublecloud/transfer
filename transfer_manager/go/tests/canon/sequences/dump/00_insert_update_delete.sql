INSERT INTO insert_update_delete(i1, i2, t) VALUES (2, 2, 'b');
UPDATE insert_update_delete SET t = 'c' WHERE i1 = 2 AND i2 = 2;
DELETE FROM insert_update_delete WHERE i1 = 2 AND i2 = 2;
