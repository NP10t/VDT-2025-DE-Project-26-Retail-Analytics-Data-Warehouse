WITH SplitPaths AS (
    SELECT 
        orderID,
        productID,
        item_position,
        path,
        path_depth,
        frequency,
        orderDate,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
),
Subsets AS (
    SELECT 
        orderID,
        productID,
        item_position,
        path,
        path_depth,
        frequency,
        orderDate,
        arraySlice(path_array, n, length(path_array) - n + 1) AS subset,
        path_array,
        n AS subset_start
    FROM SplitPaths
    ARRAY JOIN range(1, length(path_array) + 1) AS n
    WHERE length(path_array) > 0
)
SELECT 
    productID, subset, path_array, count(*) as support_count
FROM Subsets
group by path_array, productID, subset
ORDER BY 
    productID asc,
    path_array asc,
    subset asc;

----------------------------------------------



select * from efp_table
ORDER BY 
productID asc,
path asc





------------------------------------
WITH SplitPaths AS (
    SELECT 
        orderID,
        productID,
        item_position,
        path,
        path_depth,
        frequency,
        orderDate,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
    where toYYYYMM(orderDate) BETWEEN 202405 and 202407
),
Subsets AS (
    SELECT 
        orderID,
        productID,
        item_position,
        path,
        path_depth,
        frequency,
        orderDate,
        subset
    FROM SplitPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) < 6 and length(x) >= 2,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    WHERE length(path_array) > 0
)
SELECT 
    productID, subset, count(*) as support_count
FROM Subsets
group by productID, subset
ORDER BY 
    support_count asc;


WITH SplitPaths AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    -- and productID = 21
    -- and path = 'null:19'
),
Subsets AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        subset
    FROM SplitPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) >= 1,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    WHERE length(path_array) > 0
)
SELECT 
    productID, subset, count(*) as support_count
FROM Subsets
-- where length(subset) = 2
group by productID, subset
having support_count = 122
ORDER BY 
    productID desc;



WITH SplitPaths AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    and productID = 21
    and path = 'null:19'
)
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        subset
    FROM SplitPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) >= 1,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    -- WHERE length(path_array) > 0


--------------------

WITH SplitPaths AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
    where toYYYYMM(orderDate) BETWEEN 202405 and 202407
),
Subsets AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        subset
    FROM SplitPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) >= 1,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    WHERE length(path_array) > 0
)
SELECT 
    productID, subset, count(*) as support_count
FROM Subsets
-- where length(subset) = 2
group by productID, subset
-- having support_count > 50
ORDER BY 
    support_count;



WITH SplitPaths AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
    where toYYYYMM(orderDate) BETWEEN 202405 and 202407
),
Subsets AS (
    SELECT 
        orderID,
        productID,
        path,
        orderDate,
        subset
    FROM SplitPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) >= 1,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    WHERE length(path_array) > 0
)
SELECT * from Subsets
ORDER BY orderID


SELECT 
    orderID,
    productID,
    path,
    orderDate,
    arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
FROM efp_table
where toYYYYMM(orderDate) BETWEEN 202405 and 202407
ORDER BY orderID




With SplitPaths AS (
    SELECT 
        productID,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        arrayJoin(path_array) item,
        count(*) as support_count
    FROM efp_table
    -- where toYYYYMM(orderDate) BETWEEN 202405 and 202407
    group by productID, path_array
    having support_count >= 2
),
Subsets AS (
    SELECT 
        productID,
        subset
    FROM SplitPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) >= 1,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    WHERE length(path_array) > 0
)
SELECT 
    productID, subset, count(*) as support_count
FROM Subsets
group by productID, subset
having length(subset) > 3
ORDER BY 
    support_count;



SELECT 
    productID,
    arrayFilter(x -> x != 'null', splitByString(':', path)) AS unfiltered_path_array,
    arrayJoin(unfiltered_path_array) item,
    count(*) as support_count
FROM efp_table
group by productID, item
having support_count < 5




WITH ItemWithFrequency AS (
    SELECT 
        productID,
        path,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS unfiltered_path_array,
        item,
        count(*) OVER (PARTITION BY productID, item) as item_support_count
    FROM efp_table
    ARRAY JOIN arrayFilter(x -> x != 'null', splitByString(':', path)) AS item
    WHERE toYYYYMM(orderDate) BETWEEN 202405 AND 202407
),

FilteredItems AS (
    SELECT 
        productID,
        path,
        unfiltered_path_array,
        item,
        item_support_count
    FROM ItemWithFrequency
    WHERE item_support_count > 50
)
SELECT 
    productID,
    unfiltered_path_array,
    groupArray(DISTINCT item) as filtered_path_array,
    count(*) as path_frequency
FROM FilteredItems
GROUP BY productID, unfiltered_path_array
ORDER BY productID, path_frequency DESC;

WITH ItemWithCount AS (
    SELECT 
        productID,
        path,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS unfiltered_path_array,
        item,
        count(*) OVER (PARTITION BY productID, item) as item_support_count
    FROM efp_table
    ARRAY JOIN arrayFilter(x -> x != 'null', splitByString(':', path)) AS item
)
SELECT 
    productID,
    path,
    unfiltered_path_array,
    item,
    item_support_count
FROM ItemWithCount
WHERE item_support_count <= 300
group by productID, unfiltered_path_array
ORDER BY productID, unfiltered_path_array


WITH ItemWithCount AS (
    SELECT 
        productID,
        path,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS unfiltered_path_array,
        item,
        count(*) OVER (PARTITION BY productID, item) as item_support_count
    FROM efp_table
    ARRAY JOIN arrayFilter(x -> x != 'null', splitByString(':', path)) AS item
    WHERE toYYYYMM(orderDate) BETWEEN 202405 AND 202407
), frequentItems_array as (
    SELECT 
        productID,
        groupArray(item) as path_array
    FROM ItemWithCount
    WHERE item_support_count <= 300
    group by productID, unfiltered_path_array
),
Subsets AS (
    SELECT 
        productID,
        subset
    FROM frequentItems_array
    ARRAY JOIN arrayFilter(
        x -> length(x) >= 1,
        arrayMap(
        bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                path_array,
                arrayEnumerate(path_array)
            ),
        range(1, toInt32(pow(2, length(path_array))))
        )
    ) as subset
    WHERE length(path_array) > 0
)
SELECT 
    productID, subset, count(*) as support_count
FROM Subsets
group by productID, subset
having length(subset) > 3
ORDER BY 
    support_count;

------------ tối ưu --------------
WITH 
-- Tính một lần arrayFilter và cache kết quả
PreprocessedData AS (
    SELECT 
        productID,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
    WHERE toYYYYMM(orderDate) BETWEEN 202405 AND 202409
      AND path IS NOT NULL
      AND path != ''
),

-- Tính item frequency một cách hiệu quả hơn
ItemFrequency AS (
    SELECT 
        productID,
        item,
        count() as item_support_count
    FROM PreprocessedData
    ARRAY JOIN path_array AS item
    GROUP BY productID, item
    HAVING item_support_count <= 300
),

-- Tạo frequent items set cho mỗi productID
FrequentItemsSet AS (
    SELECT 
        productID,
        groupArray(item) as frequent_items
    FROM ItemFrequency
    GROUP BY productID
),

-- Filter path_array chỉ giữ frequent items
FilteredPaths AS (
    SELECT 
        pd.productID,
        arrayFilter(x -> has(COALESCE(fis.frequent_items, []), x), pd.path_array) as filtered_path_array
    FROM PreprocessedData pd
    LEFT JOIN FrequentItemsSet fis ON pd.productID = fis.productID
    WHERE length(pd.path_array) > 3  -- Early filter: chỉ xử lý path có > 3 items
),

-- Tối ưu subset generation với early filtering
Subsets AS (
    SELECT 
        productID,
        subset
    FROM FilteredPaths
    ARRAY JOIN arrayFilter(
        x -> length(x) > 3,  -- Filter ngay trong arrayFilter
        arrayMap(
            bit_mask -> arrayFilter(
                (x, i) -> bitTest(bit_mask, i-1),
                filtered_path_array,
                arrayEnumerate(filtered_path_array)
            ),
            -- Chỉ generate bit masks cho subsets có >= 4 elements
            arrayFilter(
                mask -> bitCount(mask) > 3,
                range(1, toInt32(pow(2, length(filtered_path_array))))
            )
        )
    ) as subset
    WHERE length(filtered_path_array) > 3
)

SELECT 
    productID, 
    subset, 
    count() as support_count
FROM Subsets
GROUP BY productID, subset
having support_count >= 2
ORDER BY support_count DESC;
