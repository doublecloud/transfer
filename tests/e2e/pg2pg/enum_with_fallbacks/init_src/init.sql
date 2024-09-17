create type mcae as enum ('STRING', 'NUMBER', 'ENUM');

CREATE TABLE enums(i INT PRIMARY KEY, e mcae);

INSERT INTO enums(i, e) VALUES
(1, 'STRING'),
(2, 'NUMBER'),
(3, 'ENUM');
