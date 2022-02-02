CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );
CATALOG TABLE cust ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/tpch-data/sf0.01/customer.tbl", "HEADER" = "YES", "SEPARATOR" = "|" );


DESCRIBE TABLE emp;

select name, dept_id
from emp
where emp.name = 'adarsh'
and age > 30
and dept_id > 10;

