CREATE TABLE `test` (
    `Id` int NOT NULL, -- Id, генерится в код приложения
    `Data` json NOT NULL, -- Сама сущность
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert  into `test`(`Id`,`Data`) values
(1,'{"val": 1}'),
(2,'{"val": 0}'),
(3,'{"val": 3}');
