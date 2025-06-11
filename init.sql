use ${CLICKHOUSE_DB};

CREATE NAMED COLLECTION IF NOT EXISTS minio_config AS
    access_key_id = '${MINIO_ROOT_USER}' OVERRIDABLE,
    secret_access_key = '${MINIO_ROOT_PASSWORD}' OVERRIDABLE,
    url = 'http://minio:9000/' OVERRIDABLE;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.silver
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
ORDER BY (orderID, orderDate, customerID, productID);


-- Materialized view to populate silver table from raw data
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.fact_sales
(
    orderID UInt32,
    orderDate Date,
    productID UInt32,
    customerID String,
    quantity UInt32,
    salesamount Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY (orderID, productID, orderDate, customerID); -- Chủ yếu group by orderID và join productID
                                                        -- cho nên để orderID, productID trước
                                                        -- orderDate thì đã có partition và skip index nên để sau
                                                        -- ít lọc theo customerID nên để cuối và vẫn đánh skip index

-- Thêm skip index cho productID queries
ALTER TABLE fact_sales ADD INDEX idx_product productID TYPE set(100) GRANULARITY 1; -- Tạo ra 1 set để kiểm tra nhanh input có tồn tại trong set
ALTER TABLE fact_sales ADD INDEX idx_year toYear(orderDate) TYPE minmax GRANULARITY 1;
ALTER TABLE fact_sales ADD INDEX idx_month toMonth(orderDate) TYPE minmax GRANULARITY 1;
ALTER TABLE fact_sales ADD INDEX idx_customer_bloom customerID TYPE bloom_filter(0.01) GRANULARITY 1;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.dim_products
(
    productID UInt32,
    productName LowCardinality(String) -- https://clickhouse.com/docs/sql-reference/data-types/lowcardinality
)
ENGINE = ReplacingMergeTree()
ORDER BY productID;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.dim_date
(
    orderDate Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day UInt8,
    dayOfWeek UInt8,
    isWeekend UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY orderDate;

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.fact_sales_mv
TO ${CLICKHOUSE_DB}.fact_sales
AS
SELECT
    orderID,
    orderDate,
    productID,
    customerID,
    quantity,
    salesamount
FROM ${CLICKHOUSE_DB}.silver;

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.dim_products_mv
TO ${CLICKHOUSE_DB}.dim_products
AS
SELECT DISTINCT
    productID,
    productName
FROM ${CLICKHOUSE_DB}.silver; -- Thêm Disticnt để tránh chèn trùng lặp 
                                -- trước khi Replacing merge tree giải quyết
                                -- để đỡ tốn IO

INSERT INTO ${CLICKHOUSE_DB}.dim_date
SELECT
    orderDate,
    toYear(orderDate) AS year,
    toQuarter(orderDate) AS quarter,
    toMonth(orderDate) AS month,
    toDayOfMonth(orderDate) AS day,
    toDayOfWeek(orderDate) AS dayOfWeek,
    if(toDayOfWeek(orderDate) IN (6, 7), 1, 0) AS isWeekend
FROM (
    SELECT addDays(toDate('2024-01-01'), number) as orderDate
    FROM numbers(365*2)  -- Sinh ra sẵn dữ liệu
) AS date_table;


-- 3. Tạo bảng pre-aggregated cho market basket analysis
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.order_product_sets
(
    orderID UInt32,
    year UInt16,
    month UInt8,
    customerID String,
    product_set Array(UInt32),
    product_count UInt32,
    total_amount Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY (year, month)
ORDER BY (orderID, year, month, customerID);

-- Materialized view để populate order_product_sets
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DB}.mv_order_product_sets
TO ${CLICKHOUSE_DB}.order_product_sets
AS
SELECT 
    orderID,
    toYear(orderDate) as year,
    toMonth(orderDate) as month,
    customerID,
    arraySort(groupArray(productID)) as product_set,
    count(productID) as product_count,
    sum(salesamount) as total_amount
FROM ${CLICKHOUSE_DB}.fact_sales
GROUP BY orderID, year, month, customerID;

---
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.order_product_sets_2
(
    orderID UInt32,
    orderDate DATE,
    customerID String,
    product_set Array(UInt32),
    product_count UInt32,
    total_amount Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY (orderID, orderDate, customerID);

-- Materialized view để populate order_product_sets_2
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DB}.mv_order_product_sets_2
TO ${CLICKHOUSE_DB}.order_product_sets_2
AS
SELECT 
    orderID,
    orderDate,
    customerID,
    arraySort(groupArray(productID)) as product_set,
    count(productID) as product_count,
    sum(salesamount) as total_amount
FROM ${CLICKHOUSE_DB}.fact_sales
GROUP BY orderID, orderDate, customerID;

----- aggregating array 3
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.order_product_sets_3
(
    orderID UInt32,
    orderDate Date,
    customerID String,
    product_set AggregateFunction(groupArray, UInt32),
    product_count AggregateFunction(count, UInt32),
    total_amount AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY (orderID, orderDate, customerID);

CREATE MATERIALIZED VIEW ${CLICKHOUSE_DB}.mv_order_product_sets_3
TO ${CLICKHOUSE_DB}.order_product_sets_3
AS
SELECT 
    orderID,
    orderDate,
    customerID,
    groupArrayState(productID) AS product_set,
    countState(productID) AS product_count,
    sumState(salesamount) AS total_amount
FROM ${CLICKHOUSE_DB}.fact_sales
GROUP BY orderID, orderDate, customerID;

-- bit map
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.fact_sales_bitmap
(
    orderID UInt32,
    orderDate DATE,
    customerID String,
    product_bitmap AggregateFunction(groupBitmap, UInt32),
    quantity AggregateFunction(sum, UInt32),
    salesamount AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(orderDate)
ORDER BY (orderID, orderDate, customerID);

-- Materialized view cho bitmap
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DB}.mv_fact_sales_bitmap
TO ${CLICKHOUSE_DB}.fact_sales_bitmap
AS
SELECT 
    orderID,
    orderDate,
    customerID,
    groupBitmapState(productID) as product_bitmap,
    sumState(quantity) as quantity,
    sumState(salesamount) as salesamount
FROM ${CLICKHOUSE_DB}.fact_sales
GROUP BY orderID, orderDate, customerID;
