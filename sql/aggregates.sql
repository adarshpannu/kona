CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

select dept_id, avg(age)
from emp
group by dept_id;
