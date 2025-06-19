-- Chia nhỏ data và UNION ALL kết quả

WITH chunks AS (
    SELECT 
        sub_array,
        count(*) as frequency
    FROM (
        SELECT arrayJoin(
            arrayFilter( x -> length(x) >= 2 and length(x) <= 6,
                arrayMap(bit_mask -> arrayFilter( (x, i)->bitTest(bit_mask, i-1),  arr_id, arrayEnumerate(arr_id ) ), 
                          arrayEnumerate(range(1, toInt32(pow(2, length(arr_id))))))
            )
        ) AS sub_array
        FROM (
            SELECT DISTINCT arrayMap(x -> toInt64(x), splitByString('-', productGroupID)) AS arr_id
            FROM dim_product_group
            left join fact_sales on dim_product_group.productGroupKey = fact_sales.productGroupKey
            WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 and 202412
            -- and rowNumberInAllBlocks() % 1 = 0  -- Lấy 1/4 data
        )
    )
    GROUP BY sub_array
)
SELECT 
    sub_array,
    sum(frequency) as total_frequency
FROM chunks
GROUP BY sub_array
ORDER BY total_frequency DESC

CREATE TEMPORARY TABLE subset_frequencies (
    sub_array Array(Int64),
    total_frequency UInt64
) ENGINE = Memory;

INSERT INTO subset_frequencies
WITH chunks AS (
    SELECT 
        sub_array,
        count(*) AS frequency
    FROM (
        SELECT arrayJoin(
            arrayFilter(
                x -> length(x) >= 2 AND length(x) <= 6,
                arrayMap(
                    bit_mask -> arrayFilter(
                        (x, i) -> bitTest(bit_mask, i - 1),
                        arr_id,
                        arrayEnumerate(arr_id)
                    ),
                    arrayEnumerate(range(1, toInt32(pow(2, length(arr_id)))))
                )
            )
        ) AS sub_array
        FROM (
            SELECT arrayMap(x -> toInt64OrNull(x), splitByString('-', productGroupID)) AS arr_id
            FROM dim_product_group
            LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
            WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
            GROUP BY orderID, productGroupID
        )
    )
    GROUP BY sub_array
)
SELECT 
    sub_array,
    sum(frequency) AS total_frequency
FROM chunks
GROUP BY sub_array
ORDER BY total_frequency DESC;

truncate table subset_frequencies;

select sum(total_frequency) as total_frequency
from subset_frequencies
group by sub_array
ORDER BY total_frequency DESC;

select * from subset_frequencies limit 5;

-- không đủ ram để order by final_frequency
with most_frequent_subsets
AS (
    SELECT 
        sub_array,
        sum(total_frequency) AS final_frequency
    FROM subset_frequencies
    GROUP BY sub_array
    HAVING final_frequency >= 50
)
select groupArray(productName) as productNames,
       final_frequency
from (
SELECT 
sub_array,
final_frequency,
productName
from most_frequent_subsets
array join most_frequent_subsets.sub_array as item_id
join dim_products
on item_id = dim_products.productID
)
GROUP BY sub_array, 



SELECT
    sub_array,
    sum(total_frequency) AS final_frequency
FROM subset_frequencies
WHERE has(sub_array, 19) AND has(sub_array, 85)
and length(sub_array) = 2
GROUP BY sub_array

--------- debug -------------
INSERT INTO subset_frequencies
WITH product_groups_in_period AS (
    SELECT productGroupID
    FROM dim_product_group
    LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
    WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
    GROUP BY orderID, productGroupID
),
chunks AS (
    SELECT 
        sub_array,
        count(*) AS frequency  -- Đếm số lần subset xuất hiện
    FROM (
        SELECT arrayJoin(
            arrayFilter(
                x -> length(x) >= 2 AND length(x) <= 6,
                arrayMap(
                    bit_mask -> arrayFilter(
                        (x, i) -> bitTest(bit_mask, i - 1),
                        arr_id,
                        arrayEnumerate(arr_id)
                    ),
                    arrayEnumerate(range(1, toInt32(pow(2, length(arr_id)))))
                )
            )
        ) AS sub_array
        FROM (
            SELECT arrayMap(x -> toInt64OrNull(x), splitByString('-', productGroupID)) AS arr_id
            FROM product_groups_in_period
        )
    )
    GROUP BY sub_array
)
SELECT 
    sub_array,
    sum(frequency) AS total_frequency
FROM chunks
GROUP BY sub_array
ORDER BY total_frequency DESC;

SELECT count(*) as total_rows
FROM dim_product_group
LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey

SELECT count(productGroupID) as total_rows
FROM dim_product_group
LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
GROUP BY orderID, productGroupID;
-- Showed 1000 out of 55448 rows.

SELECT count(productGroupID) as total_rows
FROM dim_product_group
LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
GROUP BY productGroupID;
-- Showed 1000 out of 47718 rows.

SELECT count(productGroupID) as total_rows
FROM dim_product_group
LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
and match(productGroupName, 'Shampoo')
and match(productGroupName, 'Corn')
GROUP BY orderID, productGroupID;


---------------------------------
--------- Replacing Merge Tree------------
WITH order_products AS (
    SELECT 
      arraySort(product_set) products
    FROM order_product_sets_2
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    having product_count >= 2
)
    SELECT 
        length(combo) as combo_size,
        combo,
        count(*) as support_count,
        count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
    FROM order_products
    ARRAY JOIN 
        arrayFilter(x -> length(x) >= 2, 
            arrayMap(i -> 
                arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                range(1, toUInt32(pow(2, length(products))))
            )
        ) as combo
GROUP BY combo_size, combo
HAVING support_count >= 2
ORDER BY support_count DESC, combo_size DESC
LIMIT 5;


WITH order_products AS (
    SELECT 
      arraySort(product_set) products
    FROM order_product_sets_2
    where toYYYYMM(orderDate) BETWEEN 202409 and 2024012
    having product_count >= 2
)
SELECT 
    combo, 
    count(*) as support_count
FROM order_products
ARRAY JOIN 
    arrayFilter(x -> length(x) >= 2 AND length(x) <= 6,
        arrayMap(i -> 
            arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
            range(1, toUInt32(pow(2, length(products))))
        )
    ) as combo
GROUP BY combo

HAVING support_count >= 2
ORDER BY support_count DESC, combo DESC
LIMIT 5;


WITH product_groups_in_period AS (
    SELECT 
      arraySort(product_set) products
    FROM order_product_sets_2
    where toYYYYMM(orderDate) BETWEEN 202409 and 202412
    having product_count >= 2
)
SELECT 
    sub_array,
    count(*) AS frequency  -- Đếm số lần subset xuất hiện
FROM (
    SELECT arrayJoin(
        arrayFilter(
            x -> length(x) >= 2 AND length(x) <= 6,
            arrayMap(
                bit_mask -> arrayFilter(
                    (x, i) -> bitTest(bit_mask, i - 1),
                    arr_id,
                    arrayEnumerate(arr_id)
                ),
                arrayEnumerate(range(1, toInt32(pow(2, length(arr_id)))))
            )
        )
    ) AS sub_array
    FROM (
        SELECT products AS arr_id
        FROM product_groups_in_period
    )
)
GROUP BY sub_array
ORDER BY frequency DESC



WITH product_groups_in_period AS (
    SELECT productGroupID
    FROM dim_product_group
    LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
    WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
    GROUP BY orderID, productGroupID
)
SELECT 
    sub_array,
    count(*) AS frequency
FROM (
    SELECT arrayJoin(
        arrayFilter(
            x -> length(x) >= 2 AND length(x) <= 6,
            arrayMap(
                bit_mask -> arrayFilter(
                    (x, i) -> bitTest(bit_mask, i - 1),
                    arr_id,
                    arrayEnumerate(arr_id)
                ),
                arrayEnumerate(range(1, toInt32(pow(2, length(arr_id)))))
            )
        )
    ) AS sub_array
    FROM (
        SELECT arrayMap(x -> toInt64OrNull(x), splitByString('-', productGroupID)) AS arr_id
        FROM product_groups_in_period
    )
)
GROUP BY sub_array
ORDER BY frequency DESC



select count(products)
from (
    SELECT 
      arraySort(product_set) products
    FROM order_product_sets_2
    where toYYYYMM(orderDate) BETWEEN 202401 and 202412
    having product_count >= 2
)

SELECT productGroupID
    FROM dim_product_group
    LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
    WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 AND 202412
    GROUP BY orderID, productGroupID

SELECT 
    arraySort(product_set) products
FROM order_product_sets_2
where toYYYYMM(orderDate) BETWEEN 202401 and 202412