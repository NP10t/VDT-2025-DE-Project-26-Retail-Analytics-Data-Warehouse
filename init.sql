use ${CLICKHOUSE_DB};

CREATE NAMED COLLECTION IF NOT EXISTS minio_config AS
    access_key_id = '${MINIO_ROOT_USER}' OVERRIDABLE,
    secret_access_key = '${MINIO_ROOT_PASSWORD}' OVERRIDABLE,
    url = 'http://minio:9000/' OVERRIDABLE;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.silver
(
    orderID String,
    orderdate Date,
    productID String,
    productName String,
    customerID String,
    quantity Int32,
    salesamount Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (orderID, orderdate, customerID, productID);


-- Materialized view to populate silver table from raw data
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.fact_orders
(
    orderID String,
    orderdate Date,
    productID String,
    customerID String,
    quantity Int32,
    salesamount Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(orderdate) -- Cải thiện hiệu suất khi truy vấn hoặc xóa dữ liệu cũ
ORDER BY (orderID, orderdate, customerID, productID);

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.dim_products
(
    productID String,
    productName String
)
ENGINE = ReplacingMergeTree()
ORDER BY productID;

-- Tìm hiểu được kỹ thuật điền dữ liệu cho 1 khoảng thời gian trước rồi fact trỏ tới dim_date sau
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.dim_date
(
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day UInt8,
    day_of_week String,
    is_weekend UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY (date);

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.fact_orders_mv
TO ${CLICKHOUSE_DB}.fact_orders
AS
SELECT
    orderID,
    orderdate,
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
FROM ${CLICKHOUSE_DB}.silver; -- Thêm Disticnt để tránh chèn trùng lặp trước khi Replacing merge tree giải quyết

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.dim_date_mv
TO ${CLICKHOUSE_DB}.dim_date
AS
SELECT DISTINCT
    orderdate AS date,
    toYear(orderdate) AS year,
    toQuarter(orderdate) AS quarter,
    toMonth(orderdate) AS month,
    toDayOfMonth(orderdate) AS day,
    toDayOfWeek(orderdate) AS day_of_week,
    if(toDayOfWeek(orderdate) IN (6, 7), 1, 0) AS is_weekend
FROM ${CLICKHOUSE_DB}.silver; -- Thêm Distince để tránh Insert trùng lặp

-- CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.product_groups
-- (
--     itemset Array(String), -- Danh sách productID trong nhóm
--     co_occurrence_count UInt32, -- Số lần xuất hiện cùng nhau
-- )

-- Aggregated table for gold data
-- This table will store pre-aggregated data for faster queries

-- CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.gold_aggregates
-- (
--     orderdate Date,
--     productID String,
--     total_quantity AggregateFunction(sum, Int32),
--     total_salesamount AggregateFunction(sum, Float64)
-- )
-- ENGINE = AggregatingMergeTree()
-- ORDER BY (orderdate, productID);

-- CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.gold_aggregates_mv
-- TO ${CLICKHOUSE_DB}.gold_aggregates
-- AS
-- SELECT
--     orderdate,
--     productID,
--     sumState(quantity) AS total_quantity,
--     sumState(salesamount) AS total_salesamount
-- FROM ${CLICKHOUSE_DB}.gold
-- GROUP BY orderdate, productID;