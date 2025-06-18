select count(*) from
(
SELECT DISTINCT e1.orderID, e1.path, e1.productID
FROM efp_table e1
JOIN fact_sales fs1
    ON e1.orderID = fs1.orderID and fs1.productID = 40
WHERE e1.productID >= 40
  and  e1.path REGEXP 'null:.*:20:.*:30:.*'
  )