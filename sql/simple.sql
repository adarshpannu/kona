CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", 
                    "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

SELECT dept_id, name from EMP
where dept_id = (2 / 2 + 8 / 2) and name != 'kate'
;

