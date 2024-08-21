-- needs to be sure there is db1
create table __test (
    id int not null primary key,
    astr varchar(10),
    bstr varchar(10),
    cstr varchar(10)
);

insert into __test values
(-1, 'astr-1', 'bstr-1', 'cstr-1'),
(1, 'astr1', 'bstr1', 'cstr1'),
(2, 'astr2', 'bstr2', 'cstr2'),
(3, 'astr3', 'bstr3', 'cstr3');
