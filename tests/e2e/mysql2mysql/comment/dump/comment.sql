CREATE TABLE `comment_test` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'a;tricky\'com;ment;;\';x;x\'',
  `txt` varchar(36) DEFAULT NULL COMMENT "do\"u;s;a;\"ble;d;;\"quotes\"\"\";\";s;\";",
  PRIMARY KEY (`id`)
);

insert into comment_test (txt) values ('\'b;\';;sd;\'l;');
insert into comment_test (txt) values ('\";x\';;\"d;\";sd;d\"\'\"\"sdf;\";\"a;\';');
