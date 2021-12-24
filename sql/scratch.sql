//select ql.partno, ql.descr, q2.suppno
//from inventory q1, quotations q2
//where q1.partno = q2.partno and q1.descr = 'engine';

SELECT DISTINCT q1.partno, ql.descr, q2.suppno
FROM inventory ql, quotations q2
WHERE ql .partno = q2.partno AND q1.descr='engine'
AND q2.price <= ALL
( SELECT q3.price FROM quotations q3
	 WHERE q2.partno=q3.partno);


