CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

SET parse_only = "true";

select sum(age)*1.1 / count(age), name
from emp
where age > 50
group by dept_id
having sum(age) > 10
;


