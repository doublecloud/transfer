CREATE TABLE testtable (
    id INTEGER PRIMARY KEY,
    value text
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (id) (
    PARTITION testtable_2009_05 VALUES LESS THAN (733893) ENGINE = InnoDB,
    PARTITION testtable_now VALUES LESS THAN MAXVALUE ENGINE = InnoDB
)*/
;
