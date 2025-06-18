SELECT 
    item_id,
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



INSERT INTO transactions (
    transaction_id, 
    item_id, 
    item_name, 
    timestamp, 
    user_id, 
    category, 
    value
) VALUES 
    (1, 'A', 'Milk', '2025-06-01 10:00:00', 101, 'Dairy', 2.5),
    (1, 'B', 'Bread', '2025-06-01 10:00:00', 101, 'Bakery', 1.5),
    (1, 'C', 'Eggs', '2025-06-01 10:00:00', 101, 'Dairy', 3.0),
    (2, 'A', 'Milk', '2025-06-01 11:00:00', 102, 'Dairy', 2.5),
    (2, 'B', 'Bread', '2025-06-01 11:00:00', 102, 'Bakery', 1.5),
    (3, 'B', 'Bread', '2025-06-01 12:00:00', 103, 'Bakery', 1.5),
    (3, 'C', 'Eggs', '2025-06-01 12:00:00', 103, 'Dairy', 3.0),
    (4, 'A', 'Milk', '2025-06-01 13:00:00', 104, 'Dairy', 2.5),
    (4, 'C', 'Eggs', '2025-06-01 13:00:00', 104, 'Dairy', 3.0);


INSERT INTO transactions (transaction_id, item_id, item_name, timestamp, user_id, category, value) 
FORMAT Values
(1, 'A', 'Milk', '2025-06-01 09:15:00', 101, 'Dairy', 2.5),
(1, 'B', 'Bread', '2025-06-01 09:15:00', 101, 'Bakery', 1.5),
(1, 'C', 'Eggs', '2025-06-01 09:15:00', 101, 'Dairy', 3.0),
(1, 'D', 'Apple', '2025-06-01 09:15:00', 101, 'Fruit', 0.8),
(1, 'E', 'Orange Juice', '2025-06-01 09:15:00', 101, 'Beverage', 4.0),
(2, 'A', 'Milk', '2025-06-01 10:30:00', 102, 'Dairy', 2.5),
(2, 'B', 'Bread', '2025-06-01 10:30:00', 102, 'Bakery', 1.5),
(2, 'F', 'Banana', '2025-06-01 10:30:00', 102, 'Fruit', 1.2),
(2, 'G', 'Canned Tuna', '2025-06-01 10:30:00', 102, 'Canned Goods', 2.0),
(3, 'B', 'Bread', '2025-06-02 11:00:00', 103, 'Bakery', 1.5),
(3, 'C', 'Eggs', '2025-06-02 11:00:00', 103, 'Dairy', 3.0),
(3, 'H', 'Chips', '2025-06-02 11:00:00', 103, 'Snacks', 2.5),
(4, 'A', 'Milk', '2025-06-02 14:20:00', 104, 'Dairy', 2.5),
(4, 'C', 'Eggs', '2025-06-02 14:20:00', 104, 'Dairy', 3.0),
(4, 'D', 'Apple', '2025-06-02 14:20:00', 104, 'Fruit', 0.8),
(4, 'E', 'Orange Juice', '2025-06-02 14:20:00', 104, 'Beverage', 4.0),
(4, 'I', 'Pasta', '2025-06-02 14:20:00', 104, 'Pasta', 1.8),
(4, 'J', 'Tomato Sauce', '2025-06-02 14:20:00', 104, 'Canned Goods', 2.2),
(5, 'F', 'Banana', '2025-06-03 08:45:00', 105, 'Fruit', 1.2),
(5, 'H', 'Chips', '2025-06-03 08:45:00', 105, 'Snacks', 2.5),
(6, 'A', 'Milk', '2025-06-03 12:10:00', 106, 'Dairy', 2.5),
(6, 'B', 'Bread', '2025-06-03 12:10:00', 106, 'Bakery', 1.5),
(6, 'G', 'Canned Tuna', '2025-06-03 12:10:00', 106, 'Canned Goods', 2.0),
(6, 'K', 'Cola', '2025-06-03 12:10:00', 106, 'Beverage', 1.9),
(7, 'C', 'Eggs', '2025-06-04 15:00:00', 107, 'Dairy', 3.0),
(7, 'D', 'Apple', '2025-06-04 15:00:00', 107, 'Fruit', 0.8),
(7, 'H', 'Chips', '2025-06-04 15:00:00', 107, 'Snacks', 2.5),
(8, 'A', 'Milk', '2025-06-04 16:30:00', 108, 'Dairy', 2.5),
(8, 'B', 'Bread', '2025-06-04 16:30:00', 108, 'Bakery', 1.5),
(8, 'E', 'Orange Juice', '2025-06-04 16:30:00', 108, 'Beverage', 4.0),
(8, 'I', 'Pasta', '2025-06-04 16:30:00', 108, 'Pasta', 1.8),
(8, 'J', 'Tomato Sauce', '2025-06-04 16:30:00', 108, 'Canned Goods', 2.2),
(9, 'B', 'Bread', '2025-06-05 09:00:00', 109, 'Bakery', 1.5),
(9, 'F', 'Banana', '2025-06-05 09:00:00', 109, 'Fruit', 1.2),
(9, 'G', 'Canned Tuna', '2025-06-05 09:00:00', 109, 'Canned Goods', 2.0),
(9, 'K', 'Cola', '2025-06-05 09:00:00', 109, 'Beverage', 1.9),
(10, 'A', 'Milk', '2025-06-05 11:45:00', 110, 'Dairy', 2.5),
(10, 'C', 'Eggs', '2025-06-05 11:45:00', 110, 'Dairy', 3.0),
(10, 'H', 'Chips', '2025-06-05 11:45:00', 110, 'Snacks', 2.5);

SYSTEM REFRESH VIEW mv_efp_construction

select * from efp_table order by item_id, path


WITH SplitPaths AS (
    SELECT 
        transaction_id,
        item_id,
        item_position,
        path,
        path_depth,
        frequency,
        timestamp,
        arrayFilter(x -> x != 'null', splitByString(':', path)) AS path_array
    FROM efp_table
),
Subsets AS (
    SELECT 
        transaction_id,
        item_id,
        item_position,
        path,
        path_depth,
        frequency,
        timestamp,
        arraySlice(path_array, n, length(path_array) - n + 1) AS subset,
        n AS subset_start
    FROM SplitPaths
    ARRAY JOIN range(1, length(path_array) + 1) AS n
    WHERE length(path_array) > 0
)
SELECT 
    item_id, subset, count(*) as support_count
FROM Subsets
group by item_id, subset
ORDER BY 
    item_id ASC,
    subset ASC;