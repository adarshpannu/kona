CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", 
                    "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

SELECT dept_id * 100, name, name from EMP
where dept_id = 8 / 2 //or name = 'kate'
;

