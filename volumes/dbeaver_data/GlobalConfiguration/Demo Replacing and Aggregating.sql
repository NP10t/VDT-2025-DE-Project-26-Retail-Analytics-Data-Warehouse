INSERT INTO fact_sales (orderID, orderDate, customerID, productID, quantity, salesamount)
VALUES (1, '2026-12-02', 'C002', 103, 3, 30.0);
  
SELECT 
    orderID,
    year,
    month,
    product_set,
    product_count,
    total_amount
FROM order_product_sets
WHERE year = 2026 AND month = 12 limit 2;

OPTIMIZE TABLE order_product_sets FINAL;

SELECT 
    orderID,
    orderDate,
    customerID,
    bitmapCardinality(groupBitmapMergeState(product_bitmap)) AS product_count,
    sumMerge(quantity) AS total_quantity,
    sumMerge(salesamount) AS total_sales
FROM fact_sales_bitmap
WHERE toYYYYMM(orderDate) = 202612
GROUP BY orderID, orderDate, customerID
LIMIT 10;

OPTIMIZE TABLE fact_sales_bitmap FINAL;