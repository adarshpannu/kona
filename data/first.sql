CATALOG TABLE cust ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/tpch-data/sf0.01/customer.tbl", "HEADER" = "YES", "SEPARATOR" = "|" );

DESCRIBE TABLE cust;

SELECT col1 AS colalias, col2 FROM emp WHERE col3 > 10 and col4 > 20;
