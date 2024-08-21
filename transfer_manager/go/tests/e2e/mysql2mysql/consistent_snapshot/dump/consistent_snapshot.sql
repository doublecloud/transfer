CREATE TABLE IF NOT EXISTS fruit (
    fruit_id INT(10) UNSIGNED NOT NULL auto_increment,
    name VARCHAR(50) NOT NULL,
    variety VARCHAR(50) NOT NULL,
    PRIMARY KEY (fruit_id)
);

INSERT INTO
    fruit (fruit_id, name, variety)
VALUES
    (1, 'Apple', 'Red Delicious'),
    (2, 'Pear', 'Comice'),
    (3, 'Orange', 'Navel'),
    (4, 'Pear', 'Bartlett'),
    (5, 'Orange', 'Blood'),
    (6, 'Apple', 'Cox''s Orange Pippin'),
    (7, 'Apple', 'Granny Smith'),
    (8, 'Pear', 'Anjou'),
    (9, 'Orange', 'Valencia'),
    (10, 'Banana', 'Plantain'),
    (11, 'Banana', 'Burro'),
    (12, 'Banana', 'Cavendish');

CREATE TABLE employee (
    id INT NOT NULL AUTO_INCREMENT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    job_title VARCHAR(100) DEFAULT NULL,
    salary DOUBLE DEFAULT NULL,
    notes text,
    PRIMARY KEY (id)
);

INSERT INTO
    employee (first_name, last_name, job_title, salary)
VALUES
    ('Robin', 'Jackman', 'Software Engineer', 5500),
    ('Taylor', 'Edward', 'Software Architect', 7200),
    ('Vivian', 'Dickens', 'Database Administrator', 6000),
    ('Harry', 'Clifford', 'Database Administrator', 6800),
    ('Eliza', 'Clifford', 'Software Engineer', 4750),
    ('Nancy', 'Newman', 'Software Engineer', 5100),
    ('Melinda', 'Clifford', 'Project Manager', 8500),
    ('Harley', 'Gilbert', 'Software Architect', 8000);
