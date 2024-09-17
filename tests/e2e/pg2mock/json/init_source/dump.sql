create table testtable2 (
                            id integer primary key,
                            val_json json,
                            val_jsonb jsonb
);
insert into testtable2 (id, val_json, val_jsonb) values (1, '{"k": 123}', '{"k": 123}');
insert into testtable2 (id, val_json, val_jsonb) values (2, '{"k": 234.5}', '{"k": 234.5}');
