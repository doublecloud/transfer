CREATE TABLE `api_reports_offline`
(
    `taskid`           bigint(20)       NOT NULL AUTO_INCREMENT,
    `ClientID`         int(10) unsigned NOT NULL,
    `ReportName`       varchar(255)     NOT NULL,
    PRIMARY KEY (`taskid`),
    UNIQUE KEY `i_ClientID_ReportName` (`ClientID`, `ReportName`)
    ) ENGINE = InnoDB
    AUTO_INCREMENT = 31793242
    DEFAULT CHARSET = utf8;

insert into `api_reports_offline` (taskid, ClientID, ReportName)
values (1, 1, 'test'),
       (2, 2, 'test'),
       (3, 3, 'test');
