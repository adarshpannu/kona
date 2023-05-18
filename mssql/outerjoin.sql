use adarshdb;
GO

select *
from EMP
go

select *
from SALARY
go

SELECT EMP.*, ROLE.*, SALARY.*
FROM    (EMP
        LEFT JOIN ROLE
        ON EMP.id = ROLE.id)
        LEFT OUTER JOIN (SALARY LEFT JOIN EMP E2
        ON SALARY.row_id = E2.row_id)
        on EMP.row_id = SALARY.row_id
ORDER BY EMP.id, ROLE.role
GO

SELECT * FROM t1 LEFT JOIN (t2, t3, t4)
                 ON (t2.a=t1.a AND t3.b=t1.b AND t4.c=t1.c)
GO