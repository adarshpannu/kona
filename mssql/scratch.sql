SELECT c.customer_num, c.company, c.phone, o.order_date
   FROM (select * from customer c)