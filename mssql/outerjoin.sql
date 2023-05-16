use adarshdb;
GO

select *
from adarshdb.adarshschema.T1
go

select *
from adarshdb.adarshschema.T2
go

SELECT T1.*, T2.*
--, T3.*
FROM adarshdb.adarshschema.T1 T1
        LEFT JOIN adarshdb.adarshschema.T2 T2
        ON T1.id = T2.id --and T1.num + T2.num = 7
--LEFT OUTER JOIN adarshdb.adarshschema.T3 T3
--ON T2.role = T3.role 
--and T2.role = 'Executive'
--WHERE T2.id = T2.id
ORDER BY T1.id, T2.role
GO
