WITH order_products AS (
    SELECT DISTINCT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
), 
common_products AS (
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
),
-- Tạo các tập hợp con bằng cách sử dụng bit manipulation
subsets AS (
    SELECT 
        orderID1,
        orderID2,
        shared_products,
        range(1, toUInt32(pow(2, length(shared_products)))) as subset_indices
    FROM common_products
),
expanded_subsets AS (
    SELECT 
        orderID1,
        orderID2,
        shared_products,
        arrayJoin(subset_indices) as subset_idx,
        -- Tạo subset dựa trên bit pattern
        arrayFilter(
            (x, i) -> bitTest(subset_idx, i - 1),
            shared_products,
            arrayEnumerate(shared_products)
        ) as subset_products
    FROM subsets
),
-- Kết hợp tập gốc và các tập con
all_sets AS (
    -- Tập gốc
    SELECT orderID1,
            orderID2,
            shared_products as product_set
    FROM common_products
    
    UNION ALL
    
    -- Các tập con
    SELECT orderID1,
            orderID2,
            subset_products as product_set
    FROM expanded_subsets
    WHERE length(subset_products) > 0  -- Loại bỏ tập rỗng
),
product_names AS (
    SELECT 
        s.orderID1,
        s.orderID2,
        s.product_set,
        groupArray(p.productName) AS product_name_set
    FROM all_sets s
    ARRAY JOIN s.product_set AS product_id
    LEFT JOIN dim_products p
        ON product_id = p.productID
    GROUP BY s.orderID1, s.orderID2, s.product_set
)
SELECT 
    orderID1,
    orderID2,
    length(product_set) AS product_count,
    product_set,
    product_name_set
FROM product_names
ORDER BY orderID1;


----------
WITH order_products AS (
    SELECT DISTINCT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
), 
common_products AS (
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
),
-- Tạo các tập hợp con bằng cách sử dụng bit manipulation
subsets AS (
    SELECT 
        orderID1,
        orderID2,
        shared_products,
        range(1, toUInt32(pow(2, length(shared_products)))) as subset_indices
    FROM common_products
),
expanded_subsets AS (
    SELECT 
        orderID1,
        orderID2,
        shared_products,
        arrayJoin(subset_indices) as subset_idx,
        -- Tạo subset dựa trên bit pattern
        arrayFilter(
            (x, i) -> bitTest(subset_idx, i - 1),
            shared_products,
            arrayEnumerate(shared_products)
        ) as subset_products
    FROM subsets
),
-- Kết hợp tập gốc và các tập con
all_sets AS (
    -- Tập gốc
    SELECT orderID1,
            orderID2,
            shared_products as product_set
    FROM common_products
    
    UNION DISTINCT 
    
    -- Các tập con
    SELECT orderID1,
            orderID2,
            subset_products as product_set
    FROM expanded_subsets
    WHERE length(subset_products) > 0  -- Loại bỏ tập rỗng
),
product_names AS (
    SELECT 
        s.orderID1,
        s.orderID2,
        s.product_set,
        -- groupArray(p.productName) AS product_name_set
    FROM all_sets s
    ARRAY JOIN s.product_set AS product_id
    -- LEFT JOIN dim_products p
    --     ON product_id = p.productID
    -- GROUP BY s.orderID1, s.orderID2, s.product_set
)
SELECT count(orderID1)
    -- orderID1,
    -- orderID2,
    -- length(product_set) AS product_count,
    -- product_set,
    -- product_name_set
FROM product_names
-- where product_count = 4

---------------
WITH order_products AS (
    SELECT DISTINCT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
), 
common_products AS (
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
),
-- Tạo các tập hợp con bằng cách sử dụng bit manipulation
subsets AS (
    SELECT 
        orderID1,
        orderID2,
        shared_products,
        range(1, toUInt32(pow(2, length(shared_products)))) as subset_indices
    FROM common_products
),
expanded_subsets AS (
    SELECT 
        orderID1,
        orderID2,
        shared_products,
        arrayJoin(subset_indices) as subset_idx,
        -- Tạo subset dựa trên bit pattern
        arrayFilter(
            (x, i) -> bitTest(subset_idx, i - 1),
            shared_products,
            arrayEnumerate(shared_products)
        ) as subset_products
    FROM subsets
),
-- Kết hợp tập gốc và các tập con
all_sets AS (
    SELECT orderID1,
            orderID2,
            subset_products as product_set
    FROM expanded_subsets
    WHERE length(subset_products) > 0  -- Loại bỏ tập rỗng
),
product_names AS (
    SELECT 
        s.orderID1,
        s.orderID2,
        s.product_set,
        groupArray(p.productName) AS product_name_set
    FROM all_sets s
    ARRAY JOIN s.product_set AS product_id
    LEFT JOIN dim_products p
        ON product_id = p.productID
    GROUP BY s.orderID1, s.orderID2, s.product_set
)
SELECT
    orderID1,
    orderID2,
    length(product_set) AS product_count,
    product_set,
    product_name_set
FROM product_names
where product_count = 4
ORDER BY orderID1;

