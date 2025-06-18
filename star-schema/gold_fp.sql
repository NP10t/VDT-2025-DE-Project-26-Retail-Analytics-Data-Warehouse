use vdt;

-- 1. Bảng giao dịch gốc (Raw Transactions)
CREATE TABLE IF NOT EXISTS silver
(
    orderID UInt32,
    orderDate Date,
    productID UInt32,
    productName String,
    customerID String,
    quantity UInt32,
    salesamount Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY (orderID, productID, orderDate, customerID);

-- 2. Bảng frequent items (để lọc items đạt minimum support)
CREATE TABLE IF NOT EXISTS frequent_items (
    productID UInt32,
    support_count UInt32,
    total_transactions UInt32
    -- support_ratio Float32,
    -- min_support_threshold Float32
) ENGINE = ReplacingMergeTree()
ORDER BY productID;

-- 3. Bảng EFP chính (Extended FP Table)
CREATE TABLE IF NOT EXISTS efp_table (
    orderID UInt64,
    productID UInt32,
    item_position UInt16,
    path String,
    path_depth UInt16,
    frequency UInt32,
    orderDate Date
) ENGINE = MergeTree()
ORDER BY (productID, path, orderID)
PARTITION BY toYYYYMM(orderDate);

-- 4. Bảng FP được tổng hợp từ EFP
CREATE TABLE IF NOT EXISTS fp_table (
    productID UInt32,
    path String,
    support_count UInt32,
    path_depth UInt16,
    last_updated Date
) ENGINE = SummingMergeTree(support_count)
ORDER BY (productID, path)
PARTITION BY path_depth;

-- 5. Bảng frequent patterns được khai thác
CREATE TABLE IF NOT EXISTS frequent_patterns (
    pattern_id String,
    itemset Array(String),
    itemset_size UInt16,
    support_count UInt32,
    confidence Float32,
    lift Float32,
    conviction Float32,
    created_at Date
) ENGINE = ReplacingMergeTree()
ORDER BY (pattern_id, itemset_size)
PARTITION BY itemset_size;

-- 6. Materialized View để xây dựng frequent items tự động
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_frequent_items
TO frequent_items
AS SELECT 
    productID,
    count() as support_count,
    (SELECT count(DISTINCT orderID) FROM silver) as total_transactions
    -- count() / (SELECT count(DISTINCT orderID) FROM silver) as support_ratio,
    -- 0.05 as min_support_threshold -- 1% minimum support
FROM silver
GROUP BY productID;
-- HAVING support_ratio >= min_support_threshold;

-- 7. Materialized View để xây dựng EFP table
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_efp_construction
REFRESH EVERY 2 DAYS
TO efp_table
AS WITH 
frequent_only AS (
    SELECT t.orderID, t.productID, t.orderDate
    FROM silver t
    INNER JOIN frequent_items f ON t.productID = f.productID
),
ordered_items AS (
    SELECT 
        orderID,
        productID,
        orderDate,
        row_number() OVER (PARTITION BY orderID ORDER BY productID) as item_position
    FROM frequent_only
),
path_construction AS (
    SELECT 
        orderID,
        productID,
        item_position,
        orderDate,
        CASE 
            WHEN item_position = 1 THEN 'null'
            ELSE concat('null:', arrayStringConcat(
                arraySlice(
                    groupArray(productID) OVER (
                        PARTITION BY orderID 
                        ORDER BY item_position 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ), 1, item_position-1
                ), ':'
            ))
        END as path
    FROM ordered_items
)
SELECT 
    orderID,
    productID,
    item_position,
    path,
    length(splitByString(':', path)) as path_depth,
    1 as frequency,
    orderDate
FROM path_construction;