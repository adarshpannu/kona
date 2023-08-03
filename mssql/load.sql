

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

if exists (select * from sysobjects where name = 'DEPT')
begin
    drop table DEPT_DETAILS
end

create table DEPT_DETAILS (
    details_dept_id int,
    location varchar(30),
);

BULK INSERT DEPT_DETAILS
FROM '/dept_details.csv'
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

select sum(age + 10)*99 / count(age + 50), avg(age + 50), emp_dept_id + 55, max(distinct age), emp_dept_id * 2, max(emp_dept_id * 2), max(name)
from emp E
where 
    age > 30 
and emp_dept_id < 99
group by emp_dept_id + 55, emp_dept_id * 2
having sum(age) > 100 and emp_dept_id + 55 > 10
;


select sum(E.age + 50)*99 / count(E.age + 50), avg(E.age), D.dept_id, sum(E.age)
from emp E, dept D, dept_details DD
where 
    E.age > 20 
and D.dept_id < 99
and E.emp_dept_id = D.dept_id
and D.dept_id = DD.details_dept_id
and D.name = 'Engineering'
group by D.dept_id
having sum(E.age) > 100 and D.dept_id < 10
;

select * from emp E1, emp E2;



