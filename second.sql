// foo bar

//cataLog TABLE cust ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/tpch-data/sf0.01/customer.tbl", "HEADER" = "NO", "SEPARATOR" = "|" );
//DESCRIBE TABLE cust;

CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

WITH
 CTE1 AS (SELECT col1 FROM emp)
SELECT col111 
FROM CTE1, (SELECT col2 FROM emp)
;




SELECT
