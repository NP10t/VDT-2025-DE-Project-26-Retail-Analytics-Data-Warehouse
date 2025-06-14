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


-- case when 
select sum(satisfied) from
(
select case when hasAll(groupArrayMerge(product_set), (select groupArray(productID) as ids from dim_products 
where productName in ('Cheese', 'Yogurt') ) ) then 3 else 0 end satisfied
from order_product_sets_3
where toYYYYMM(orderDate) = 202405
group by orderID
) 



select countIf(matched_item_cnt = 2) * 100.0 / (
                  select uniqHLL12(orderID) 
                  from fact_sales 
                  where toYYYYMM(orderDate) = 202405
                  )
from
(select count(*) as matched_item_cnt
from fact_sales fs
join dim_products dp on fs.productID = dp.productID
where toYYYYMM(orderDate) = 202405
and dp.productName In ('Cheese', 'Eggs')
group by fs.orderID ) as groupOrder



select sum(satisfied) * 100 /count() from
(
select hasAll(groupArrayMerge(product_set), 
                ( select groupArray(productID) as ids 
                from dim_products 
                where productName in ['Cheese', 'Eggs']  ) as xx
              ) satisfied
from order_product_sets_3
where toYYYYMM(orderDate) = 202405
group by orderID
)



select countIf(matched_item_cnt = 2) * 100.0 / (
                  select uniqHLL12(orderID) 
                  from fact_sales 
                  where toYYYYMM(orderDate) = 202405
                  )
from
(select count(*) as matched_item_cnt
from fact_sales fs
join dim_products dp on fs.productID = dp.productID
where toYYYYMM(orderDate) = 202405
and dp.productName In ('Cheese', 'Eggs')
group by fs.orderID ) as groupOrder



SELECT
    LEAST(t1.productName, t2.productName) AS product_1,
    GREATEST(t1.productName, t2.productName) AS product_2,
    COUNT(DISTINCT fs1.orderID) AS order_count,
    COUNT(DISTINCT fs1.orderID) * 100.0 / (
        SELECT COUNT(DISTINCT orderID)
        FROM fact_sales
        WHERE toYYYYMM(orderDate) = 202405
    ) AS percentage
FROM fact_sales fs1
JOIN dim_products t1 ON fs1.productID = t1.productID
JOIN fact_sales fs2 ON fs1.orderID = fs2.orderID AND fs1.productID < fs2.productID
JOIN dim_products t2 ON fs2.productID = t2.productID
WHERE toYYYYMM(fs1.orderDate) = 202405
GROUP BY LEAST(t1.productName, t2.productName), GREATEST(t1.productName, t2.productName)
HAVING order_count >= 10
ORDER BY order_count DESC
LIMIT 10;


SELECT
    CONCAT(product_1, ' + ', product_2) AS product_pair,
    MAX(order_count) AS order_count,
    MAX(percentage) AS percentage
FROM (
    SELECT
        LEAST(t1.productName, t2.productName) AS product_1,
        GREATEST(t1.productName, t2.productName) AS product_2,
        COUNT(DISTINCT fs1.orderID) AS order_count,
        COUNT(DISTINCT fs1.orderID) * 100.0 / (
            SELECT COUNT(DISTINCT orderID)
            FROM fact_sales
            WHERE toYYYYMM(orderDate) = 202405
        ) AS percentage
    FROM fact_sales fs1
    JOIN dim_products t1 ON fs1.productID = t1.productID
    JOIN fact_sales fs2 ON fs1.orderID = fs2.orderID AND fs1.productID < fs2.productID
    JOIN dim_products t2 ON fs2.productID = t2.productID
    WHERE toYYYYMM(fs1.orderDate) = 202405
    GROUP BY 
        LEAST(t1.productName, t2.productName),
        GREATEST(t1.productName, t2.productName)
    HAVING order_count >= 10
) AS subquery
GROUP BY product_1, product_2
ORDER BY order_count DESC
LIMIT 10;


WITH order_products AS (
    SELECT DISTINCT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
)
SELECT 
    a.orderID as orderID1,
    b.orderID as orderID2,
    COUNT(*) as common_products,
    arraySort(groupArray(a.productID)) as shared_products,
    sipHash64(arrayStringConcat(arraySort(groupArray(a.productID)), ',')) AS shared_products_hash
FROM order_products a
INNER JOIN order_products b 
    ON a.productID = b.productID 
    AND a.orderID < b.orderID
GROUP BY a.orderID, b.orderID
HAVING common_products > 2
ORDER BY shared_products_hash
LIMIT 20;


WITH order_products AS (
    SELECT DISTINCT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
), common_products as(
SELECT 
    a.orderID as orderID1,
    b.orderID as orderID2,
    COUNT(*) as common_products_cnt,
    arraySort(groupArray(a.productID)) as shared_products
FROM order_products a
INNER JOIN order_products b 
    ON a.productID = b.productID 
    AND a.orderID < b.orderID
GROUP BY a.orderID, b.orderID
HAVING common_products_cnt > 1
)
select count(orderID1), shared_products
from common_products
group by shared_products