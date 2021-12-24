//select ql.partno, ql.descr, q2.suppno
//from inventory q1, quotations q2
//where q1.partno = q2.partno and q1.descr = 'engine';


SELECT SUBSTRING(C_PHONE,1,2) AS CNTRYCODE, C_ACCTBAL
FROM CUSTOMER 
WHERE SUBSTRING(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17') 
AND C_ACCTBAL > (SELECT AVG(C_ACCTBAL) FROM CUSTOMER WHERE C_ACCTBAL > 0 
AND SUBSTRING(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) 
AND EXISTS ( SELECT * FROM ORDERS WHERE O_CUSTKEY = C_CUSTKEY)
