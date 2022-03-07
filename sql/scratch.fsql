CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );
//CATALOG TABLE cust ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/tpch-data/sf0.01/customer.tbl", "HEADER" = "YES", "SEPARATOR" = "|" );


DESCRIBE TABLE emp;

select E.name, E2.dept_id
from emp E, emp E2
where E.name = 'adarsh'
and E2.age > 30
and E.dept_id > 10
and E2.dept_id < 15;

