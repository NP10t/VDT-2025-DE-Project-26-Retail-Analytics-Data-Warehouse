WITH target_products AS (
    SELECT productID
    FROM dim_products
    WHERE productName IN ('Chips', 'Yogurt')
),
product_count AS (
    SELECT count(*) AS target_count
    FROM target_products
)
SELECT (
    SELECT count() AS group_count
    FROM (
        SELECT fs.orderID
        FROM fact_sales fs
        INNER JOIN target_products tp ON fs.productID = tp.productID
        where year(fs.orderDate) = 2025
        GROUP BY fs.orderID
        HAVING count(DISTINCT fs.productID) = (SELECT target_count FROM product_count)
    )
) / (
    SELECT count(DISTINCT orderID) AS total_order_count
    FROM fact_sales fs
    where year(fs.orderDate) = 2025
);