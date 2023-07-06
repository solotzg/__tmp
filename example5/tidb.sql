create database IF NOT EXISTS demo;
create table IF NOT EXISTS demo.t5(a int PRIMARY KEY, b int);
create table IF NOT EXISTS demo.t5_build(b int PRIMARY KEY, c int);
insert into demo.t5 values(1,1),(2,2),(3,3),(4,2);
insert into demo.t5_build values(1,-1),(2,-2),(3,-3);
