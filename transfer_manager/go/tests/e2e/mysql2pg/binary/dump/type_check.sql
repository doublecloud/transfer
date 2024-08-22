-- needs to be sure there is db1
create table __test (
    `Id` binary(16) NOT NULL,
    `Version` int(11) NOT NULL,
    `Data` json NOT NULL,
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into __test values (0x8E1CF5E9084080E811ECA1542DE42988, 1, '{"а": "1"}');
insert into __test values (X'DAEBFCCC2D07B6B611ECA15454969110', 2, '{"а": "2"}');
insert into __test values (X'DAEBFCCC2D07B6B611ECA15454969111', 3, '{"а": "3"}');

--

create table __test2 (
    id      bigint(20),
    created timestamp,
    digest  binary(16),
    rnd     int(11),
    url     varbinary(65000),
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into __test2 (id, created, digest, rnd, url) values (82790, '2012-11-15 06:13:58Z', X'856fa595bedb6e12aae3789661e2f935', 48, X'2f7468726561642f333637534831325f663937333835343974393734343731305f6d74613f6465706172747572653d323031332d30312d3031');
insert into __test2 (id, created, digest, rnd, url) values (121162, '2016-06-18T12:37:31.000000Z', X'b86fa11d6154d23dcc6334f13667bf55', 44, X'746573742E363736');
