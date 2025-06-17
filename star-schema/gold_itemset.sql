use vdt;


CREATE TABLE IF NOT EXISTS fact_itemset (
    -- Itemset information
    itemset_id String,              -- SHA256 hash của itemset (32 bytes hex)
    itemset_size UInt8,             -- Kích thước itemset (2-6)
    
    -- Order information  
    orderID String,
    customerID String,
    orderDate Date    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY (itemset_size, itemset_id, orderID)  -- Optimize cho queries by size và itemset
SETTINGS index_granularity = 8192;

-- Index phụ để tối ưu queries
ALTER TABLE fact_itemset ADD INDEX idx_customer_date (customerID, orderDate) TYPE minmax GRANULARITY 4;
ALTER TABLE fact_itemset ADD INDEX idx_itemset_hash (cityHash64(itemset_id)) TYPE bloom_filter GRANULARITY 1;

-- =====================================================
-- MATERIALIZED VIEW - REAL TIME PROCESSING
-- =====================================================

-- Materialized View để tự động populate fact_itemset từ silver table
CREATE MATERIALIZED VIEW mv_fact_itemset
TO fact_itemset
AS
WITH 
    orders AS (
        SELECT 
            orderID,
            customerID, 
            orderDate,
            count(*) as item_cnt,
            groupArray(productID) as all_items
        FROM silver
        GROUP BY orderID, customerID, orderDate
    ),
    itemsets AS (
        SELECT 
            -- Đổi từ SHA256 sang sipHash64 để tối ưu performance
            hex(sipHash128(arrayStringConcat(arraySort(itemset), '|'))) as itemset_id,
            itemset,
            length(itemset) as itemset_size,
            orderID, 
            customerID, 
            orderDate,
        FROM orders
        ARRAY JOIN arrayFilter(
            x -> length(x) BETWEEN 2 AND 6,  -- Chỉ lấy itemsets size 2-6
            arrayMap(
                bit_mask -> arrayFilter(
                    (x, i) -> bitTest(bit_mask, i-1),
                    all_items,
                    arrayEnumerate(all_items)
                ),
                range(1, toInt32(pow(2, item_cnt)))
            )
        ) AS itemset
    )
SELECT 
    itemset_id,
    itemset_size,
    orderID,
    customerID, 
    orderDate,
FROM itemsets
WHERE itemset_size BETWEEN 2 AND 6;