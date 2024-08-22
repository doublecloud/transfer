DROP TABLE IF EXISTS insert_update_delete;
CREATE TABLE insert_update_delete(i1 INT, i2 INT, t TEXT, PRIMARY KEY(i1, i2));
ALTER TABLE insert_update_delete REPLICA IDENTITY FULL;
