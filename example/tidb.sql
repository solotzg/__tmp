create database IF NOT EXISTS demo;
create table IF NOT EXISTS demo.t1(a int PRIMARY KEY);
insert into demo.t1 values(1),(2),(3);
insert into demo.t1 (select max(a)+1 from demo.t1);