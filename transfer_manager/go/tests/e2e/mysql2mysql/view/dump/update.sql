CREATE TABLE test (
                       id INT PRIMARY KEY AUTO_INCREMENT,
                       name VARCHAR(50),
                       email VARCHAR(100),
                       age INT
);

CREATE TABLE test2 (
                      id INT PRIMARY KEY AUTO_INCREMENT,
                      name VARCHAR(50),
                      email VARCHAR(100),
                      age INT
);

INSERT INTO test(name, email, age) VALUES ('Franklin', 'mailadam', 71);
INSERT INTO test(name, email, age) VALUES ('not Franklin', 'test', 20);
INSERT INTO test2(name, email, age) VALUES ('Adam', 'mail', 21);
INSERT INTO test2(name, email, age) VALUES ('Not Adam', 'test2', 37);

CREATE VIEW test_view (v_name, v_age, v_email) AS SELECT test.name, test.age, test.email FROM test;
CREATE VIEW test_view2 (v_name1, v_age1, v_email2) AS SELECT test.name, test.age, test2.email FROM test, test2;


-- We get views by alphabetical order in GetViewDDLs(...) transfer_manager/go/pkg/providers/mysql/schema_copy.go
-- So for such queries:
CREATE VIEW b AS SELECT * FROM test; -- DO NOT RENAME VIEW without reading comments
CREATE VIEW a AS SELECT * FROM b;    -- DO NOT RENAME VIEW without reading comments

-- DDLs will be in other order: firstly code will try to CREATE VIEW a ... FROM b.
-- So by those queries we check logic of applyDDLs's dependence handling.