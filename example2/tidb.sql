create database IF NOT EXISTS demo;
create table IF NOT EXISTS demo.t2(a int PRIMARY KEY, b TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, c varchar(20));
insert into demo.t2 (a,c) values(1,'1'),(2,'2'),(3, null);
