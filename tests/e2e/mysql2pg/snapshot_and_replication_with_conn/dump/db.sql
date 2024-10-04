-- needs to be sure there is db1
create table test (
    id bigint,
    val varchar(255),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into test values (1, 'foo');
insert into test values (2, 'bar');
