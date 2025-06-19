--------------------- order_product_sets_2 ------------
WITH product_groups_in_period AS (
    SELECT 
      arraySort(product_set) arr_id
    FROM order_product_sets_2
    where toYYYYMM(orderDate) BETWEEN 202401 and 202412
    having product_count >= 2
),
mining as (
    SELECT 
        sub_array, 
        count(*) as support_count
    FROM product_groups_in_period
    ARRAY JOIN 
        arrayFilter(
            x -> length(x) >= 2 AND length(x) <= 6,
            arrayMap(
                bit_mask -> arrayFilter(
                    (x, i) -> bitTest(bit_mask, i - 1),
                    arr_id,
                    arrayEnumerate(arr_id)
                ),
                range(1, toInt32(pow(2, length(arr_id))))
            )
        ) as sub_array
    GROUP BY sub_array
)
SELECT * FROM mining
WHERE support_count > 50
LIMIT 5;
-- 5 rows in set. Elapsed: 4.776 sec. Processed 55.45 thousand rows, 
-- 2.00 MB (11.61 thousand rows/s., 417.96 KB/s.)       
-- Peak memory usage: 2.10 GiB.


WITH product_groups_in_period AS (
    SELECT arrayMap(x -> toInt64OrNull(x), splitByString('-', productGroupID)) AS arr_id
    FROM dim_product_group
    LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
    WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
    GROUP BY orderID, productGroupID
),
mining as (
    SELECT 
        sub_array, 
        count(*) as support_count
    FROM product_groups_in_period
    ARRAY JOIN 
        arrayFilter(
            x -> length(x) >= 2 AND length(x) <= 6,
            arrayMap(
                bit_mask -> arrayFilter(
                    (x, i) -> bitTest(bit_mask, i - 1),
                    arr_id,
                    arrayEnumerate(arr_id)
                ),
                range(1, toInt32(pow(2, length(arr_id))))
            )
        ) as sub_array
    GROUP BY sub_array
)
SELECT * FROM mining
WHERE support_count > 50
LIMIT 5;
-- 5 rows in set. Elapsed: 28.044 sec. Processed 381.78 thousand rows, 
-- 19.50 MB (13.61 thousand rows/s., 695.31 KB/s.)
-- Peak memory usage: 3.38 GiB.
