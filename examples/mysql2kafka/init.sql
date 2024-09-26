GRANT REPLICATION CLIENT ON *.* TO 'testuser'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'testuser'@'%';

FLUSH PRIVILEGES;

CREATE TABLE users (
   id INT AUTO_INCREMENT PRIMARY KEY,
   name VARCHAR(100),
   email VARCHAR(100)
);

INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.com');
