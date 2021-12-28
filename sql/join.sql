CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );
CATALOG TABLE dept ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/dept.csv", "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;
DESCRIBE TABLE dept;

// All engineers aged 
SELECT EMP.name, DEPT.DEPT_ID 
from EMP, DEPT
where EMP.DEPT_ID = DEPT.DEPT_ID
AND EMP.age > 35
AND DEPT.NAME = "Engineering"
;

SELECT SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PART

WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BOX'

AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY)
;
