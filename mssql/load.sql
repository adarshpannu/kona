
use adarshdb;
GO

if exists (select * from sysobjects where name = 'EMP')
begin
    drop table EMP
end

create table EMP (
    name varchar(30),
    age int,
    emp_dept_id int
);

BULK INSERT EMP
FROM '/emp.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO

select * from EMP;
GO

if exists (select * from sysobjects where name = 'DEPT')
begin
    drop table DEPT
end

create table DEPT (
    dept_id int,
    name varchar(30),
    org_id int,
);

BULK INSERT DEPT
FROM '/dept.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO

select * from DEPT;
GO

SELECT EMP.age, EMP.name, DEPT.DEPT_ID
from EMP, DEPT
where EMP.EMP_DEPT_ID = DEPT.DEPT_ID
AND EMP.age > 35
AND DEPT.NAME = 'Engineering'
;

select emp_dept_id + 55, emp_dept_id * 2
from emp
where 
    age > 50 
and emp_dept_id < 99
;
