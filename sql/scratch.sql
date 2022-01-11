

WITH cte_org AS (
    SELECT 
        e.staff_id as FOO,
        e.first_name,
        e.manager_id
    FROM 
        sales.staffs e
        , cte_org o 
            where o.staff_id = e.manager_id and col1 is not null
)
SELECT * FROM cte_org;
