CATALOG TABLE emp ( "TYPE" = "CSV", "PATH" = "/Users/adarshrp/Projects/flare/data/emp.csv", "HEADER" = "YES", "SEPARATOR" = "," );

DESCRIBE TABLE emp;

SELECT * from EMP
where dept_id = 1;


select R3, S3
FROM R, S
where R.R1 = 10
and S.S1 = 20
and R.R2 = S.S2
;
