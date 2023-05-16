

use master;

if exists (select * from sysdatabases where name = 'adarshdb')
begin
    drop database adarshdb
end

CREATE DATABASE adarshdb;
GO

use adarshdb;
GO

CREATE SCHEMA adarshschema;
go

if exists (select * from sysobjects where name = 'T1')
begin
    drop table T1
end

create table adarshdb.adarshschema.T1 (
    id int,
    name varchar(30),
    num int
);

if exists (select * from sysobjects where name = 'T2')
begin
    drop table T2
end

create table adarshdb.adarshschema.T2 (
    id int,
    role varchar(30),
    num int

);

if exists (select * from sysobjects where name = 'T3')
begin
    drop table T2
end

create table adarshdb.adarshschema.T3 (
    role varchar(30),
    salary int,
    num int

);

insert into adarshdb.adarshschema.T1 values (10, 'Adarsh', 1);
insert into adarshdb.adarshschema.T1 values (20, 'Anjul', 2);
insert into adarshdb.adarshschema.T1 values (30, 'Ankita', 3);

insert into adarshdb.adarshschema.T2 values (20, 'Executive', 4);
insert into adarshdb.adarshschema.T2 values (20, 'Manager', 5);
insert into adarshdb.adarshschema.T2 values (30, 'Engineer', 6);
insert into adarshdb.adarshschema.T2 values (40, 'Janitor', 7);
GO

insert into adarshdb.adarshschema.T3 values ('CEO', 500, 8);
insert into adarshdb.adarshschema.T3 values ('Executive', 100, 9);
insert into adarshdb.adarshschema.T3 values ('Manager', 50, 10);
insert into adarshdb.adarshschema.T3 values ('Engineer', 30, 11);

select * from adarshdb.adarshschema.T1;
select * from adarshdb.adarshschema.T2;
select * from adarshdb.adarshschema.T3;

GO
