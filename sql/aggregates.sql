CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

SET parse_only = "true";

with cte1 as (select * from emp)
select sum(age + 10)*1.1 / count(age + 50), dept_id + 55, avg(age)
from emp, cte1, (select * from emp)
where age > 50
group by dept_id + 55
having sum(age) > 100
;

