

use master;

if exists (select * from sysdatabases where name = 'adarshdb')
begin
    drop database adarshdb
end

CREATE DATABASE adarshdb;
GO

use adarshdb;
GO

if exists (select * from sysobjects where name = 'EMP')
begin
    drop table EMP
end

create table EMP (
    id int,
    name varchar(30),
    row_id int
);

if exists (select * from sysobjects where name = 'ROLE')
begin
    drop table ROLE
end

create table ROLE (
    id int,
    role varchar(30),
    row_id int

);

if exists (select * from sysobjects where name = 'SALARY')
begin
    drop table ROLE
end

create table SALARY (
    role varchar(30),
    salary int,
    row_id int

);

insert into EMP values (10, 'Adarsh', 1);
insert into EMP values (20, 'Anjul', 2);
insert into EMP values (30, 'Ankita', 3);

insert into ROLE values (20, 'Executive', 4);
insert into ROLE values (20, 'Manager', 5);
insert into ROLE values (30, 'Engineer', 6);
insert into ROLE values (40, 'Janitor', 7);
GO

insert into SALARY values ('CEO', 500, 8);
insert into SALARY values ('Executive', 100, 9);
insert into SALARY values ('Manager', 50, 10);
insert into SALARY values ('Engineer', 30, 11);

select * from EMP;
select * from ROLE;
select * from SALARY;

GO
