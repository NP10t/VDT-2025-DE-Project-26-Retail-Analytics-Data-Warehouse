WITH product_IDs as (
    SELECT DISTINCT productID
    FROM dim_products
    WHERE productName IN ('Chips', 'Yogurt')
    ),
    group_orders as (
        SELECT orderID
        FROM fact_sales
        WHERE productID IN product_IDs
        GROUP BY orderID
        HAVING count(DISTINCT productID) = (select count(*) from product_IDs)
    ),
    order_count as (
        select count(*) as group_order_count
        from group_orders
    ),
    total_orders AS (
        SELECT COUNT(DISTINCT orderID) AS total_order_count
        FROM fact_sales
    )
SELECT
    order_count.group_order_count / total_orders.total_order_count AS group_order_ratio
FROM total_orders, order_count






SELECT 
    group_count * 1.0 / total_order_count AS group_order_ratio
FROM
(
    SELECT count() AS group_count
    FROM
    (
        SELECT orderID
        FROM fact_sales
        INNER JOIN dim_products ON fact_sales.productID = dim_products.productID
        WHERE dim_products.productName IN ('Chips', 'Yogurt')
        GROUP BY orderID
        HAVING count(DISTINCT productID) = 2
    )
) AS group_sub,
(
    SELECT count(DISTINCT orderID) AS total_order_count
    FROM fact_sales
) AS total_sub;


SELECT (
    SELECT count()
    FROM (
        SELECT orderID
        FROM fact_sales as fs
        INNER JOIN dim_products as dp ON fs.productID = dp.productID
        WHERE dp.productName IN ('Chips', 'Yogurt')
        GROUP BY orderID
        HAVING count(DISTINCT productID) = 2
    )
) / (
    SELECT count(DISTINCT orderID)
    FROM fact_sales
);





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
        GROUP BY fs.orderID
        HAVING count(DISTINCT fs.productID) = (SELECT target_count FROM product_count)
    )
) / (
    SELECT count(DISTINCT orderID) AS total_order_count
    FROM fact_sales 
);








WITH target_products AS (
    SELECT productID
    FROM dim_products
    WHERE productName IN ('Chips', 'Yogurt')
)
SELECT (
    SELECT count() AS group_count
    FROM (
        SELECT fact_sales.orderID
        FROM fact_sales
        INNER JOIN target_products ON fact_sales.productID = target_products.productID
        GROUP BY fact_sales.orderID
        HAVING count(DISTINCT fact_sales.productID) = (SELECT count(*) FROM target_products)
    )
) / (
    SELECT count(DISTINCT orderID) AS total_order_count
    FROM fact_sales 
);



SELECT orderID, groupArray(dp.productName) AS products
FROM fact_sales fs
INNER JOIN dim_products dp ON fs.productID = dp.productID
GROUP BY orderID
HAVING length(products) >= 3
limit 2


SELECT COUNT(fo1.orderID) as count_concur, fo1.productID prod_1, fo2.productID prod_2
FROM fact_sales fo1
JOIN fact_sales fo2 ON fo1.orderID = fo2.orderID
WHERE fo1.productID < fo2.productID
GROUP BY fo1.productID, fo2.productID
ORDER BY count_concur DESC
LIMIT 10


SELECT
    product_triplet,
    count(*) AS order_count
FROM (
    SELECT
        arraySort([p1, p2, p3]) AS product_triplet
    FROM (
        SELECT
            dp_list[1] AS p1,
            dp_list[2] AS p2,
            dp_list[3] AS p3
        FROM (
            SELECT
                arrayJoin(combinations) AS dp_list
            FROM (
                SELECT
                    arrayJoin(arrayEnumerate(groupArray(dp.productName))) AS i,
                    groupArray(dp.productName) AS all_products,
                    arrayMap(i -> arraySlice(all_products, i, 3), range(length(all_products) - 2)) AS combinations
                FROM fact_sales fs
                INNER JOIN dim_products dp ON fs.productID = dp.productID
                GROUP BY fs.orderID
                HAVING length(groupArray(dp.productName)) >= 3
            )
        )
    )
)
GROUP BY product_triplet
ORDER BY order_count DESC
LIMIT 1;


CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.product_pairs
(
    product1_id String,
    product2_id String,
    co_occurrence_count UInt32,
)
ENGINE = AggregatingMergeTree()
ORDER BY (product1_id, product2_id);