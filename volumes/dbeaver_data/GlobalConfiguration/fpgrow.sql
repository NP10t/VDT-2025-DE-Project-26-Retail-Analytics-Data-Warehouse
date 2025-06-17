-- 1. Bảng giao dịch gốc (Raw silver)
CREATE TABLE silver (
    orderID UInt64,
    productID String,
    item_name String,
    timestamp DateTime,
    user_id UInt64,
    category String,
    value Float32
) ENGINE = MergeTree()
ORDER BY (orderID, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- 2. Bảng frequent items (để lọc items đạt minimum support)
CREATE TABLE frequent_items (
    productID String,
    support_count UInt32,
    total_transactions UInt32,
    support_ratio Float32,
    min_support_threshold Float32
) ENGINE = ReplacingMergeTree()
ORDER BY productID;

-- 3. Bảng EFP chính (Extended FP Table)
CREATE TABLE efp_table (
    orderID UInt64,
    productID String,
    item_position UInt16,
    path String,
    path_depth UInt16,
    frequency UInt32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (productID, path, orderID)
PARTITION BY toYYYYMM(timestamp);

-- 4. Bảng FP được tổng hợp từ EFP
CREATE TABLE fp_table (
    productID String,
    path String,
    support_count UInt32,
    path_depth UInt16,
    last_updated DateTime
) ENGINE = SummingMergeTree(support_count)
ORDER BY (productID, path)
PARTITION BY path_depth;

-- 5. Bảng frequent patterns được khai thác
CREATE TABLE frequent_patterns (
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
CREATE MATERIALIZED VIEW mv_frequent_items
TO frequent_items
AS SELECT 
    productID,
    count() as support_count,
    (SELECT count(DISTINCT orderID) FROM silver) as total_transactions,
    count() / (SELECT count(DISTINCT orderID) FROM silver) as support_ratio,
    0.01 as min_support_threshold -- 1% minimum support
FROM silver
GROUP BY productID
HAVING support_ratio >= 0.01;

-- 7. Materialized View để xây dựng EFP table
CREATE MATERIALIZED VIEW mv_efp_construction
TO efp_table
AS WITH 
frequent_only AS (
    SELECT t.orderID, t.productID, t.timestamp
    FROM silver t
    INNER JOIN frequent_items f ON t.productID = f.productID
),
ordered_items AS (
    SELECT 
        orderID,
        productID,
        timestamp,
        row_number() OVER (PARTITION BY orderID ORDER BY productID) as item_position
    FROM frequent_only
),
path_construction AS (
    SELECT 
        orderID,
        productID,
        item_position,
        timestamp,
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
    timestamp
FROM path_construction;

-- 8. Function để tính toán association rules
CREATE OR REPLACE FUNCTION calculate_association_rules(
    min_support Float32,
    min_confidence Float32
) AS '''
WITH itemset_support AS (
    SELECT 
        itemset,
        support_count,
        support_count / (SELECT count(DISTINCT orderID) FROM silver) as support
    FROM frequent_patterns
    WHERE itemset_size >= 2
),
rule_generation AS (
    SELECT 
        i1.itemset as antecedent,
        arrayFilter(x -> NOT has(i1.itemset, x), i2.itemset) as consequent,
        i2.support_count as rule_support,
        i1.support_count as antecedent_support,
        i2.support_count / i1.support_count as confidence
    FROM itemset_support i1
    CROSS JOIN itemset_support i2
    WHERE length(i1.itemset) < length(i2.itemset)
    AND arraySlice(i2.itemset, 1, length(i1.itemset)) = i1.itemset
)
SELECT 
    antecedent,
    consequent,
    rule_support,
    antecedent_support,
    confidence,
    rule_support / (
        SELECT support_count FROM itemset_support 
        WHERE itemset = consequent
    ) as lift
FROM rule_generation
WHERE confidence >= min_confidence
ORDER BY confidence DESC, lift DESC;
''';

-- 9. Stored procedure để chạy FP-Growth algorithm
CREATE OR REPLACE FUNCTION run_fp_growth(
    min_support_threshold Float32
) AS '''
-- Step 1: Build FP table from EFP
INSERT INTO fp_table
SELECT 
    productID,
    path,
    sum(frequency) as support_count,
    max(path_depth) as path_depth,
    now() as last_updated
FROM efp_table
GROUP BY productID, path
HAVING support_count >= (
    SELECT count(DISTINCT orderID) * min_support_threshold 
    FROM silver
);

-- Step 2: Mine frequent patterns recursively
-- (Simplified version - full implementation would require recursive mining)
INSERT INTO frequent_patterns
SELECT 
    concat('pattern_', toString(rowNumberInAllBlocks())) as pattern_id,
    groupArray(productID) as itemset,
    count() as itemset_size,
    min(support_count) as support_count,
    0.0 as confidence,
    0.0 as lift,
    0.0 as conviction,
    now() as created_at
FROM fp_table
WHERE support_count >= (
    SELECT count(DISTINCT orderID) * min_support_threshold 
    FROM silver
)
GROUP BY path
HAVING itemset_size >= 2;
''';

-- 10. Indexes để tối ưu hiệu suất
ALTER TABLE silver ADD INDEX idx_item_id productID TYPE bloom_filter GRANULARITY 1;
ALTER TABLE efp_table ADD INDEX idx_path path TYPE bloom_filter GRANULARITY 1;
ALTER TABLE fp_table ADD INDEX idx_item_path (productID, path) TYPE minmax GRANULARITY 1;

-- 11. Query examples để phân tích
-- Tìm top 10 frequent items
SELECT 
    productID,
    support_count,
    support_ratio,
    support_ratio * 100 as support_percentage
FROM frequent_items
ORDER BY support_count DESC
LIMIT 10;

-- Phân tích frequent patterns theo độ dài
SELECT 
    itemset_size,
    count() as pattern_count,
    avg(support_count) as avg_support,
    max(support_count) as max_support
FROM frequent_patterns
GROUP BY itemset_size
ORDER BY itemset_size;

-- Tìm association rules mạnh nhất
SELECT 
    antecedent,
    consequent,
    confidence,
    lift,
    CASE 
        WHEN lift > 1 THEN 'Positive correlation'
        WHEN lift < 1 THEN 'Negative correlation'
        ELSE 'Independent'
    END as relationship_type
FROM calculate_association_rules(0.01, 0.5)
LIMIT 20;


SELECT 
    productID,
    arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
    sum(frequency) as support_count,
    max(path_depth) as path_depth,
    now() as last_updated
FROM efp_table
WHERE productID = 43
GROUP BY productID, path
ORDER BY productID, path



-- INSERT INTO fp_table
with fp_table as (
    SELECT 
        productID,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) as support_count,
        max(path_depth) as path_depth,
        now() as last_updated
    FROM efp_table
    where productID = 20
    GROUP BY productID, path
) SELECT 
    arrayJoin(path_array) item, sum(support_count) sum_support_count
FROM fp_table
group by productID, item
ORDER BY item



with fp_table as (
    SELECT 
        productID,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) as support_count,
        max(path_depth) as path_depth,
        now() as last_updated
    FROM efp_table
    where productID = 100
    GROUP BY productID, path
    -- HAVING support_count >= 3
) SELECT 
    path_array, sum(support_count) as sum_support_count
FROM fp_table
group by path_array
-- having support_count > 1
ORDER BY path_array




WITH RECURSIVE fp_table AS (
    SELECT 
        productID, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) as support_count
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
),
-- Base case: itemsets có 1 phần tử
itemsets_level_1 AS (
    SELECT 
        1 as level,
        [item] as itemset,
        sum(support_count) as sum_support_count
    FROM fp_table
    ARRAY JOIN path_array as item
    GROUP BY item
),
-- Recursive case: tạo itemsets với nhiều phần tử hơn
itemsets_recursive AS (
    -- Base case
    SELECT level, itemset, sum_support_count FROM itemsets_level_1
    
    UNION ALL
    
    -- Recursive step
    SELECT 
        ir.level + 1 as level,
        arrayConcat(ir.itemset, [new_item]) as itemset,
        sum(fp.support_count) as sum_support_count
    FROM itemsets_recursive ir
    CROSS JOIN (
        SELECT DISTINCT arrayJoin(path_array) as new_item 
        FROM fp_table
    ) new_items
    JOIN fp_table fp ON hasAll(fp.path_array, arrayConcat(ir.itemset, [new_items.new_item]))
    WHERE ir.level < 5  -- Giới hạn độ sâu để tránh vô hạn
        AND new_items.new_item > ir.itemset[length(ir.itemset)]  -- Tránh trùng lặp
    GROUP BY ir.level + 1, arrayConcat(ir.itemset, [new_items.new_item])
    HAVING sum_support_count > 0
)
SELECT * FROM itemsets_recursive
ORDER BY level, itemset


------------ opimize ---------
WITH RECURSIVE fp_table AS (
    SELECT 
        productID, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) as support_count
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
),
-- Base case: itemsets có 1 phần tử
itemsets_level_1 AS (
    SELECT 
        1 as level,
        [item] as itemset,
        sum(support_count) as sum_support_count
    FROM fp_table
    ARRAY JOIN path_array as item
    GROUP BY item
),
-- Recursive case: tạo itemsets với nhiều phần tử hơn
itemsets_recursive AS (
    -- Base case
    SELECT level, itemset, sum_support_count FROM itemsets_level_1
    
    UNION ALL
    
    -- Recursive step
    SELECT 
        ir.level + 1 as level,
        arrayConcat(ir.itemset, [new_item]) as itemset,
        sum(fp.support_count) as sum_support_count
    FROM itemsets_recursive ir
    CROSS JOIN (
        SELECT DISTINCT arrayJoin(path_array) as new_item 
        FROM fp_table
    ) new_items
    JOIN fp_table fp ON hasAll(fp.path_array, arrayConcat(ir.itemset, [new_items.new_item]))
    WHERE ir.level < 5  -- Giới hạn độ sâu để tránh vô hạn
        AND new_items.new_item > ir.itemset[length(ir.itemset)]  -- Tránh trùng lặp
    GROUP BY ir.level + 1, arrayConcat(ir.itemset, [new_items.new_item])
    HAVING sum_support_count > 0
)
SELECT * FROM itemsets_recursive
ORDER BY level, itemset


WITH fp_table AS (
    SELECT 
        productID,
        productID head, 
        [toString(head)] as cur_itemset,
        999999 cur_sp_cnt, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) as support_count
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
)
    SELECT 
        1 as level,
        head,
        least(cur_sp_cnt, sum(support_count) ) as sum_support_count,
        arrayConcat(cur_itemset, [item]) as itemset,
        item candidate
    FROM fp_table
    ARRAY JOIN path_array as item
    GROUP BY head, cur_sp_cnt, cur_itemset, item


WITH fp_table AS (
    SELECT 
        productID,
        productID head, 
        [toString(head)] as itemset,
        999999 cur_sp_cnt, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) as candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), level_1 as (SELECT 
    1 as level,
    head,
    least(cur_sp_cnt, candidate_frequency_1 ) as cur_sp_cnt_2,
    arrayConcat(itemset, [candidate_1]) as itemset_1,
    candidate_1,
    sum(candidate_frequency) candidate_frequency_1
    FROM fp_table
    ARRAY JOIN path_array as candidate_1
    GROUP BY head, itemset, candidate_1, cur_sp_cnt
), level_2 as (SELECT 
    1 as level,
    candidate as head,
    least(cur_sp_cnt_2, candidate_frequency_2 ) as cur_sp_cnt_3,
    arrayConcat(itemset_1, [candidate_1]) as itemset_2,
    candidate_2,
    sum(candidate_frequency_1) as candidate_frequency_2
    FROM level_1
    where item != candidate
    GROUP BY head, itemset_1, candidate_1, cur_sp_cnt_2
) select * from level_2


-----------------
WITH fp_table AS (
    SELECT 
        productID,
        productID AS head, 
        [toString(head)] AS itemset,
        999999 AS cur_sp_cnt, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) AS candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), 

level_1 AS (
    SELECT 
        1 AS level,
        head,
        least(cur_sp_cnt, sum(candidate_frequency)) AS cur_sp_cnt_2,
        arrayConcat(itemset, [candidate_1]) AS itemset_1,
        candidate_1,
        sum(candidate_frequency) AS candidate_frequency_1,
        path_array  -- Giữ lại path_array để sử dụng ở level tiếp theo
    FROM fp_table
    ARRAY JOIN path_array AS candidate_1
    GROUP BY head, itemset, candidate_1, cur_sp_cnt, path_array
), 

level_2 AS (
    SELECT 
        2 AS level,  -- Sửa level thành 2
        head,
        least(cur_sp_cnt_2, sum(candidate_frequency_1)) AS cur_sp_cnt_3,
        arrayConcat(itemset_1, [candidate_2]) AS itemset_2,
        candidate_2,
        sum(candidate_frequency_1) AS candidate_frequency_2
    FROM level_1
    ARRAY JOIN path_array AS candidate_2  -- Sử dụng ARRAY JOIN với path_array
    WHERE candidate_2 != candidate_1  -- So sánh với candidate_1, không phải 'item'
      AND candidate_2 > candidate_1   -- Đảm bảo thứ tự lexicographic để tránh trùng lặp
    GROUP BY head, itemset_1, candidate_1, candidate_2, cur_sp_cnt_2
)

SELECT * FROM level_2
ORDER BY cur_sp_cnt_3 DESC, itemset_2;



WITH fp_table AS (
    SELECT 
        productID,
        productID AS head, 
        [toString(head)] AS itemset,
        999999 AS candidate_frequency_0, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) AS candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), 

level_1 AS (
    SELECT 
        1 AS level,
        head,
        least(candidate_frequency_0,  sum(candidate_frequency)) AS candidate_frequency_2,
        arrayConcat(itemset, [candidate_1]) AS itemset_1,
        candidate_1,
        path_array  -- Giữ lại path_array để sử dụng ở level tiếp theo
    FROM fp_table
    ARRAY JOIN path_array AS candidate_1
    GROUP BY head, itemset, candidate_1, candidate_frequency_0, path_array
) SELECT * from level_1

---------------------
WITH fp_table AS (
    SELECT 
        productID,
        productID AS head, 
        [toString(head)] AS itemset,
        999999 AS candidate_frequency_0, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) AS candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), 

level_1 AS (
    SELECT 
        1 AS level,
        head,
        least(candidate_frequency_0, 
              sum(candidate_frequency) OVER (PARTITION BY head, itemset, candidate_1)) AS candidate_frequency_2,
        arrayConcat(itemset, [candidate_1]) AS itemset_1,
        candidate_1,
        path_array,
        row_number() OVER (PARTITION BY head, itemset, candidate_1 ORDER BY path_array) AS rn
    FROM fp_table
    ARRAY JOIN path_array AS candidate_1
)
SELECT 
    level,
    head,
    candidate_frequency_2,
    itemset_1,
    candidate_1,
    rn
FROM level_1 
WHERE rn = 1
ORDER BY candidate_frequency_2 DESC, itemset_1;

-----------------
WITH fp_table AS (
    SELECT 
        productID,
        productID AS head, 
        [toString(head)] AS itemset,
        999999 AS candidate_frequency_0, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) AS candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), 

level_1 AS (
    SELECT 
        1 AS level,
        head,
        least(candidate_frequency_0, 
              sum(candidate_frequency) OVER (PARTITION BY head, itemset, candidate_1)) AS candidate_frequency_1,
        arrayConcat(itemset, [candidate_1]) AS itemset_1,
        candidate_1,
        path_array,
        row_number() OVER (PARTITION BY head, itemset, candidate_1 ORDER BY path_array) AS rn_1
    FROM fp_table
    ARRAY JOIN path_array AS candidate_1
), 
level_2 AS (
    SELECT 
        lv1.level+1 AS level,
        lv1_2.candidate_1 head,
        least(2, 
              sum(candidate_frequency_1) OVER (PARTITION BY head, itemset_1, candidate_2)) AS candidate_frequency_2,
        arrayConcat(itemset_1, [candidate_2]) AS itemset_2,
        lv1.candidate_1 candidate_2,
        path_array,
        row_number() OVER (PARTITION BY head, itemset_1, candidate_1 ORDER BY path_array) AS rn_2
    FROM level_1 lv1
    JOIN level_1 lv1_2 ON lv1.head = lv1_2.head
    WHERE lv1.head != lv1_2.candidate_1
        AND rn_1 = 1
)
SELECT * from level_2
where rn_2 = 1

----------------

WITH fp_table AS (
    SELECT 
        productID,
        productID AS head, 
        [toString(head)] AS itemset,
        999999 AS itemset_frequency_0, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) AS candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), 

level_1 AS (
    SELECT 
        1 AS level,
        head,
        least(itemset_frequency_0, 
              sum(candidate_frequency) OVER (PARTITION BY head, itemset, candidate_1)) AS itemset_frequency_1,
        arrayConcat(itemset, [candidate_1]) AS itemset_1,
        candidate_1,
        candidate_frequency,
        path_array,
        row_number() OVER (PARTITION BY head, itemset, candidate_1 ORDER BY path_array) AS rn_1
    FROM fp_table
    ARRAY JOIN path_array AS candidate_1
), 

level_2 AS (
    SELECT 
        lv1.level + 1 AS level,
        toString(lv1_2.candidate_1) AS head,  -- Chuyển về string để khớp với itemset
        least(itemset_frequency_1,  -- Sử dụng candidate_frequency_1 thay vì số 2
              sum(lv1_2.candidate_frequency) OVER (PARTITION BY toString(lv1_2.candidate_1), itemset_2, lv1_2.candidate_1)
              ) 
              AS itemset_frequency_2,
        arrayConcat(lv1.itemset_1, [lv1_2.candidate_1]) AS itemset_2,
        lv1_2.candidate_1 AS candidate_2,
        candidate_frequency,
        lv1.path_array,
        row_number() OVER (PARTITION BY toString(lv1_2.candidate_1), lv1.itemset_1, lv1_2.candidate_1 ORDER BY lv1.path_array) AS rn_2
    FROM level_1 lv1
    JOIN level_1 lv1_2 ON lv1.head = lv1_2.head
    WHERE lv1.candidate_1 != lv1_2.candidate_1  -- So sánh candidate_1 với candidate_1
        AND lv1.candidate_1 > lv1_2.candidate_1  -- Đảm bảo thứ tự để tránh trùng lặp
        AND lv1.rn_1 = 1
        AND lv1_2.rn_1 = 1
)

SELECT 
    level,
    head,
    itemset_frequency_2,
    itemset_2,
    candidate_2
FROM level_2
WHERE rn_2 = 1
ORDER BY itemset_frequency_2 DESC, itemset_2;


--------------

WITH fp_table AS (
    SELECT 
        productID,
        productID AS head, 
        [toString(head)] AS itemset,
        999999 AS itemset_frequency_0, 
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array,
        sum(frequency) AS candidate_frequency
    FROM efp_table 
    WHERE productID = 20
    GROUP BY productID, path
), 

level_1 AS (
    SELECT 
        1 AS level,
        head,
        least(itemset_frequency_0, 
              sum(candidate_frequency) OVER (PARTITION BY head, itemset, candidate_1)) AS itemset_frequency_1,
        arrayConcat(itemset, [candidate_1]) AS itemset_1,
        candidate_1,
        candidate_frequency,
        path_array,
        row_number() OVER (PARTITION BY head, itemset, candidate_1 ORDER BY path_array) AS rn_1
    FROM fp_table
    ARRAY JOIN path_array AS candidate_1
)
    SELECT 
        lv1.level + 1 AS level,
        toString(lv1_2.candidate_1) AS head,  -- Chuyển về string để khớp với itemset
        least(itemset_frequency_1,  -- Sử dụng candidate_frequency_1 thay vì số 2
              sum(lv1_2.candidate_frequency) OVER (PARTITION BY toString(lv1_2.candidate_1), itemset_2, lv1_2.candidate_1)
              ) 
              AS itemset_frequency_2,
        arrayConcat(lv1.itemset_1, [lv1_2.candidate_1]) AS itemset_2,
        lv1_2.candidate_1 AS candidate_2,
        candidate_frequency,
        lv1.path_array,
        row_number() OVER (PARTITION BY toString(lv1_2.candidate_1), lv1.itemset_1, lv1_2.candidate_1 ORDER BY lv1.path_array) AS rn_2
    FROM level_1 lv1
    JOIN level_1 lv1_2 ON lv1.head = lv1_2.head
    WHERE lv1.candidate_1 != lv1_2.candidate_1  -- So sánh candidate_1 với candidate_1
        AND lv1.candidate_1 > lv1_2.candidate_1  -- Đảm bảo thứ tự để tránh trùng lặp
        AND lv1.rn_1 = 1
        AND lv1_2.rn_1 = 1