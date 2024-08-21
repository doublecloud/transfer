create table products
(
	id int auto_increment
		primary key,
	title varchar(256) not null
);

insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) values (LEFT(MD5(RAND()), 250));
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 10;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 20;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 40;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 20;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 100;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 200;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 400;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 800;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 400;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 2000;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 2000;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 2000;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 2000; -- 10k rows here
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 10000;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 10000;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 10000;
insert into products (title) select LEFT(MD5(RAND()), 250) from products limit 10000; -- 50k rows here