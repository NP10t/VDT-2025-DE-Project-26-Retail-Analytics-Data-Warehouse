use vdt;

-- 1. Bảng giao dịch gốc (Raw Transactions)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UInt64,
    item_id String,
    item_name String,
    timestamp DateTime,
    user_id UInt64,
    category String,
    value Float32
) ENGINE = MergeTree()
ORDER BY (transaction_id, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- 2. Bảng frequent items (để lọc items đạt minimum support)
CREATE TABLE IF NOT EXISTS frequent_items (
    item_id String,
    support_count UInt32,
    total_transactions UInt32,
    support_ratio Float32,
    min_support_threshold Float32
) ENGINE = ReplacingMergeTree()
ORDER BY item_id;

-- 3. Bảng EFP chính (Extended FP Table)
CREATE TABLE IF NOT EXISTS efp_table (
    transaction_id UInt64,
    item_id String,
    item_position UInt16,
    path String,
    path_depth UInt16,
    frequency UInt32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (item_id, path, transaction_id)
PARTITION BY toYYYYMM(timestamp);

-- 4. Bảng FP được tổng hợp từ EFP
CREATE TABLE IF NOT EXISTS fp_table (
    item_id String,
    path String,
    support_count UInt32,
    path_depth UInt16,
    last_updated DateTime
) ENGINE = SummingMergeTree(support_count)
ORDER BY (item_id, path)
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
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY (pattern_id, itemset_size)
PARTITION BY itemset_size;

-- 6. Materialized View để xây dựng frequent items tự động
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_frequent_items
TO frequent_items
AS SELECT 
    item_id,
    count() as support_count,
    (SELECT count(DISTINCT transaction_id) FROM transactions) as total_transactions,
    count() / (SELECT count(DISTINCT transaction_id) FROM transactions) as support_ratio,
    0.01 as min_support_threshold -- 1% minimum support
FROM transactions
GROUP BY item_id
HAVING support_ratio >= 0.01;

-- 7. Materialized View để xây dựng EFP table
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_efp_construction
REFRESH EVERY 2 DAYS
TO efp_table
AS WITH 
frequent_only AS (
    SELECT t.transaction_id, t.item_id, t.timestamp
    FROM transactions t
    INNER JOIN frequent_items f ON t.item_id = f.item_id
),
ordered_items AS (
    SELECT 
        transaction_id,
        item_id,
        timestamp,
        row_number() OVER (PARTITION BY transaction_id ORDER BY item_id) as item_position
    FROM frequent_only
),
path_construction AS (
    SELECT 
        transaction_id,
        item_id,
        item_position,
        timestamp,
        CASE 
            WHEN item_position = 1 THEN 'null'
            ELSE concat('null:', arrayStringConcat(
                arraySlice(
                    groupArray(item_id) OVER (
                        PARTITION BY transaction_id 
                        ORDER BY item_position 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ), 1, item_position-1
                ), ':'
            ))
        END as path
    FROM ordered_items
)
SELECT 
    transaction_id,
    item_id,
    item_position,
    path,
    length(splitByString(':', path)) as path_depth,
    1 as frequency,
    timestamp
FROM path_construction;

-- 8. Function để tính toán association rules
-- CREATE OR REPLACE FUNCTION calculate_association_rules(
--     min_support Float32,
--     min_confidence Float32
-- ) AS '''
-- WITH itemset_support AS (
--     SELECT 
--         itemset,
--         support_count,
--         support_count / (SELECT count(DISTINCT transaction_id) FROM transactions) as support
--     FROM frequent_patterns
--     WHERE itemset_size >= 2
-- ),
-- rule_generation AS (
--     SELECT 
--         i1.itemset as antecedent,
--         arrayFilter(x -> NOT has(i1.itemset, x), i2.itemset) as consequent,
--         i2.support_count as rule_support,
--         i1.support_count as antecedent_support,
--         i2.support_count / i1.support_count as confidence
--     FROM itemset_support i1
--     CROSS JOIN itemset_support i2
--     WHERE length(i1.itemset) < length(i2.itemset)
--     AND arraySlice(i2.itemset, 1, length(i1.itemset)) = i1.itemset
-- )
-- SELECT 
--     antecedent,
--     consequent,
--     rule_support,
--     antecedent_support,
--     confidence,
--     rule_support / (
--         SELECT support_count FROM itemset_support 
--         WHERE itemset = consequent
--     ) as lift
-- FROM rule_generation
-- WHERE confidence >= min_confidence
-- ORDER BY confidence DESC, lift DESC;
-- ''';

-- -- 9. Stored procedure để chạy FP-Growth algorithm
-- CREATE OR REPLACE FUNCTION run_fp_growth(
--     min_support_threshold Float32
-- ) AS '''
-- -- Step 1: Build FP table from EFP
-- INSERT INTO fp_table
-- SELECT 
--     item_id,
--     path,
--     sum(frequency) as support_count,
--     max(path_depth) as path_depth,
--     now() as last_updated
-- FROM efp_table
-- GROUP BY item_id, path
-- HAVING support_count >= (
--     SELECT count(DISTINCT transaction_id) * min_support_threshold 
--     FROM transactions
-- );

-- -- Step 2: Mine frequent patterns recursively
-- -- (Simplified version - full implementation would require recursive mining)
-- INSERT INTO frequent_patterns
-- SELECT 
--     concat('pattern_', toString(rowNumberInAllBlocks())) as pattern_id,
--     groupArray(item_id) as itemset,
--     count() as itemset_size,
--     min(support_count) as support_count,
--     0.0 as confidence,
--     0.0 as lift,
--     0.0 as conviction,
--     now() as created_at
-- FROM fp_table
-- WHERE support_count >= (
--     SELECT count(DISTINCT transaction_id) * min_support_threshold 
--     FROM transactions
-- )
-- GROUP BY path
-- HAVING itemset_size >= 2;
-- ''';

-- 10. Indexes để tối ưu hiệu suất
-- ALTER TABLE transactions ADD INDEX idx_item_id item_id TYPE bloom_filter GRANULARITY 1;
-- ALTER TABLE efp_table ADD INDEX idx_path path TYPE bloom_filter GRANULARITY 1;
-- ALTER TABLE fp_table ADD INDEX idx_item_path (item_id, path) TYPE minmax GRANULARITY 1;
