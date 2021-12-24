SELECT a.FirstName, a.LastName  
FROM Person AS a  
WHERE SUBSTRING(C_PHONE,1,2) IN (select a rom