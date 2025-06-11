SELECT * from dim_products;

-- Silver
-- processed đủ 500 ngàn dòng (toàn bộ bảng)
-- 1 row in set. Elapsed: 0.047 sec. Processed 500.00 thousand rows, 11.45 MB (10.55 million rows/s., 241.56 MB/s.)
-- Peak memory usage: 452.98 KiB.
SELECT 
    countIf(product_count = 2) / count() as ratio
FROM (
    SELECT 
        orderID,
        count(CASE WHEN productName IN ('Eggs', 'Cheese') THEN 1 END) as product_count
    FROM silver
    WHERE toYYYYMM(orderDate) = 202407
    GROUP BY orderID
);

-- Brief_query
-- NOTE: Tuy query ngắn gọn nhưng không lọc productName trước khi join, rất lãng phi, 
-- nhưng với dữ liệu nhỏ thì không thấy được
-- Nhưng lại
-- 1 row in set. Elapsed: 0.028 sec. Processed 42.20 thousand rows, 423.14 KB (1.52 million rows/s., 15.19 MB/s.)
-- Peak memory usage: 181.79 KiB.
-- 42k vì chỉ quét 1 lần đã chia được và count cái mẫu được luôn rồi
-- Vấn đề là phải tính cái mẫu nên processed >= 42k => tính trước cái mẫu bằng materialize view
SELECT 
    countIf(product_count = 2) / count() as ratio
FROM (
    SELECT 
        fs.orderID,
        count(CASE WHEN dp.productName IN ('Eggs', 'Cheese') THEN 1 END) as product_count
    FROM fact_sales fs
    INNER JOIN dim_products dp ON dp.productID = fs.productID
    WHERE toYYYYMM(orderDate) = 202407
    GROUP BY fs.orderID
);

-- Không hiểu sao Processed lại lớn
-- 1 row in set. Elapsed: 0.053 sec. Processed 84.31 thousand rows, 675.77 KB (1.60 million rows/s., 12.83 MB/s.)
-- Peak memory usage: 300.91 KiB.
-- 84k là vì quét 2 lần, 1 lần để phép chia (42k), 1 lần để count cái mẫu số (42k)
select count() / (SELECT COUNT(DISTINCT orderID) FROM fact_sales where toYYYYMM(orderDate) = 202407)
FROM (
SELECT orderID 
FROM fact_sales fs
JOIN dim_products dp ON fs.productID = dp.productID
WHERE dp.productName in ('Eggs', 'Cheese')
AND toYYYYMM(orderDate) = 202407
GROUP BY fs.orderID
HAVING COUNT(productID) = 2
);

-- hyperloglog
-- 1 row in set. Elapsed: 0.037 sec. Processed 84.31 thousand rows, 675.88 KB (2.26 million rows/s., 18.12 MB/s.)
-- Peak memory usage: 271.13 KiB.
SELECT count() / (SELECT uniqHLL12(orderID) FROM fact_sales WHERE toYYYYMM(orderDate) = 202407)
FROM (
    SELECT orderID 
    FROM fact_sales fs
    JOIN dim_products dp ON fs.productID = dp.productID
    WHERE dp.productName IN ('Eggs', 'Cheese')
    AND toYYYYMM(orderDate) = 202407
    GROUP BY fs.orderID
    HAVING COUNT(productID) = 2
);

-- process 42.1k dòng (42k tương ứng với where toYYYYMM(orderDate) = 202407 và 0.1*100 = 100 là size product_dim) 
-- chứng tỏ phép lọc productName đã không được áp dụng cho cả 2 truy ván trên
-- 42105 rows in set. Elapsed: 0.039 sec. Processed 42.20 thousand rows, 2.82 MB (1.09 million rows/s., 73.00 MB/s.)
-- Peak memory usage: 5.04 MiB.
select * 
from dim_products dp
join fact_sales fs on dp.productID = fs.productID
WHERE toYYYYMM(orderDate) = 202407

-- Conventional_Query
-- Processed rất lớn
-- 1 row in set. Elapsed: 0.078 sec. Processed 84.41 thousand rows, 678.08 KB (1.08 million rows/s., 8.65 MB/s.)
-- Peak memory usage: 32.77 MiB.
WITH target_products AS (
    SELECT productID
    FROM dim_products
    WHERE productName IN ('Eggs', 'Cheese')
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
        where toYYYYMM(orderDate) = 202407
        GROUP BY fs.orderID
        HAVING count(DISTINCT fs.productID) = (SELECT target_count FROM product_count)
    )
) / (
    SELECT count(DISTINCT orderID) AS total_order_count
    FROM fact_sales fs
    where toYYYYMM(orderDate) = 202407
);

-- Bitmap_AggregatingMT
-- Processed nhỏ nhưng peak mem lớn
-- 1 row in set. Elapsed: 0.047 sec. Processed 7.74 thousand rows, 1.74 MB (166.15 thousand rows/s., 37.27 MB/s.)
-- Peak memory usage: 4.44 MiB.
WITH target_product_ids AS (
    SELECT groupBitmapState(productID) as target_bitmap
    FROM dim_products
    WHERE productName IN ('Eggs', 'Cheese')
)
  select countIf(intersect_cnt = 2)/count() from (
SELECT
      bitmapCardinality(
          bitmapAnd(
              groupBitmapMergeState(product_bitmap),
              (SELECT target_bitmap FROM target_product_ids)
          )
      ) intersect_cnt
FROM fact_sales_bitmap
WHERE toYYYYMM(orderDate) = 202407
GROUP BY orderID, orderDate, customerID
);

-- Array_ReplacingMT_year_month
-- Processed bé, peak bé
-- 1 row in set. Elapsed: 0.035 sec. Processed 7.74 thousand rows, 254.57 KB (221.70 thousand rows/s., 7.29 MB/s.)
-- Peak memory usage: 153.95 KiB.
with target_ids as (
  SELECT groupArray(productID)
  from dim_products
  WHERE productName in ('Eggs', 'Cheese')
)
  SELECT countIf(hasAll(product_set, (select * from target_ids) )) / count() 
  from order_product_sets
  WHERE year = 2024 and month = 7;

-- Array_ReplacingMT_YYYYMM
-- Processed bé, peak bé, giống ý chang như trên
-- 1 row in set. Elapsed: 0.024 sec. Processed 7.74 thousand rows, 246.93 KB (316.43 thousand rows/s., 10.09 MB/s.)
-- Peak memory usage: 157.13 KiB.
with target_ids as (
  SELECT groupArray(productID)
  from dim_products
  WHERE productName in ('Eggs', 'Cheese')
)
  SELECT countIf(hasAll(product_set, (select * from target_ids) )) / count() 
  from order_product_sets_2
  WHERE toYYYYMM(orderDate) = 202407;


-- Array_AggregatingMT_YYYYMM
-- processed bé, peak bé
-- 1 row in set. Elapsed: 0.024 sec. Processed 7.74 thousand rows, 866.04 KB (323.87 thousand rows/s., 36.23 MB/s.)        
-- Peak memory usage: 249.53 KiB.
WITH target_product_ids AS (
    SELECT groupArray(productID) as ids
    FROM dim_products
    WHERE productName IN ('Eggs', 'Cheese')
)
  select sum(satisfied)/count() from (
SELECT
      hasAll(
        groupArrayMerge(product_set),
        (SELECT ids FROM target_product_ids)
      ) satisfied
FROM order_product_sets_3
WHERE toYYYYMM(orderDate) = 202407
GROUP BY orderID, orderDate, customerID
);

-- tại sao processed các truy vấn trên là 7k7
-- Đó chính là số orderID
-- 1 row in set. Elapsed: 0.019 sec. Processed 42.10 thousand rows, 252.63 KB (2.19 million rows/s., 13.13 MB/s.)
-- Peak memory usage: 296.58 KiB.
select count(distinct orderID) from fact_sales where toYYYYMM(orderDate) = 202407

SELECT query_id, query
FROM clusterAllReplicas(default, system.query_log)
WHERE event_date >= (today() - 3)
    AND type = 2
    AND query LIKE '-- %'
ORDER BY event_time DESC
LIMIT 2

WITH
    initial_query_id = ( SELECT get_latest_query('-- Array_ReplacingMT_year_month%') ) as first,
    initial_query_id = ( SELECT get_latest_query('-- Array_ReplacingMT_YYYYMM%') ) as second
  SELECT metric, v1 as Array_ReplacingMT_year_month, v2 as Array_ReplacingMT_YYYYMM, dB
  from
 ( SELECT
      PE.1 AS metric,
      sumIf(PE.2, first) AS v1,
      sumIf(PE.2, second) AS v2,
      10 * log10(v2 / v1) AS dB,
      round(((v2 - v1) / if(v2 > v1, v2, v1)) * 100, 2) AS perc,
      bar(abs(perc), 0, 100, 33) AS bar
  FROM clusterAllReplicas(default, system.query_log)
  ARRAY JOIN ProfileEvents AS PE
  WHERE (first OR second) AND (event_date >= (today() - 3)) AND (type = 2)
  GROUP BY metric
  HAVING (v1 != v2) AND (abs(perc) >= 0)
  ORDER BY
      dB DESC,
      v2 DESC,
      metric ASC
);