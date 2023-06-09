create database IF NOT EXISTS demo;
create table IF NOT EXISTS demo.t1(a int PRIMARY KEY, b int);
insert into demo.t1 values(1,1),(2,2),(3,3);
insert into demo.t1 (select max(a)+1,max(a)+1 from demo.t1);