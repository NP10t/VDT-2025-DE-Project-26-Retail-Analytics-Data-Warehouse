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