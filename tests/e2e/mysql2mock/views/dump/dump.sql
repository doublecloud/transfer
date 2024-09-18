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

INSERT INTO test(name, email, age) VALUES ('Hideo Kojima', 'test', 69);
INSERT INTO test(name, email, age) VALUES ('Ya sjel deda', 'morgen', 20);
INSERT INTO test2(name, email, age) VALUES ('not deda', 'morgen2', 21);
INSERT INTO test2(name, email, age) VALUES ('Not Kojima', 'test2', 42);

CREATE VIEW test_view (v_name, v_age, v_email) AS SELECT test.name, test.age, test.email FROM test;
CREATE VIEW test_view2 (v_name1, v_age1, v_email2) AS SELECT test2.name, test2.age, test2.email FROM test2;
