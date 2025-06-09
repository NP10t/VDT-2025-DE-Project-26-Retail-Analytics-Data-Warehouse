SELECT 
    countIf(product_count = 2) / count() as ratio
FROM (
    SELECT 
        fs.orderID,
        count(CASE WHEN dp.productName IN ('Chips', 'Yogurt') THEN 1 END) as product_count
    FROM fact_sales fs
    INNER JOIN dim_date dd ON dd.dateID = fs.dateID
    INNER JOIN dim_products dp ON dp.productID = fs.productID
    WHERE dd.year = 2024
    GROUP BY fs.orderID
);
