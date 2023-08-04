create database IF NOT EXISTS demo;
create table IF NOT EXISTS demo.t4(a int PRIMARY KEY, b int);
insert into demo.t4 values(1,1),(2,2),(3,3);
insert into demo.t4 (select max(a)+1,max(a)+1 from demo.t4);