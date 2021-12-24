//select ql.partno, ql.descr, q2.suppno
//from inventory q1, quotations q2
//where q1.partno = q2.partno and q1.descr = 'engine';


SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
FROM (SELECT SUBSTRING(C_PHONE,1,2) AS CNTRYCODE, C_ACCTBAL
FROM CUSTOMER WHERE SUBSTRING(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND
C_ACCTBAL > (SELECT AVG(C_ACCTBAL) FROM CUSTOMER WHERE C_ACCTBAL > 0.00 AND
SUBSTRING(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND
NOT EXISTS ( SELECT * FROM ORDERS WHERE O_CUSTKEY = C_CUSTKEY)) AS CUSTSALE
GROUP BY CNTRYCODE
ORDER BY CNTRYCODE;

SELECT DISTINCT q1.partno, ql.descr, q2.suppno
FROM inventory ql, quotations q2
WHERE ql .partno = q2.partno AND q1.descr='engine'
AND FUN(1, 2, 3) > 10
AND q2.price <= ALL
( SELECT q3.price FROM quotations q3
	 WHERE q2.partno=q3.partno);
