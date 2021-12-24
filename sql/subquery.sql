
select name from EMP
where age >= (select age from EMP)
;
