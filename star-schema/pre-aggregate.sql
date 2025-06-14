-------------- Replacing MergeTree Year Month --------------
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

-------------- Replacing MergeTree YYYYMM --------------
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

-------------- Aggregating MergeTree YYYYMM --------------
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

-- Materialized view để populate order_product_sets_3
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

-------------- Aggregating MergeTree Bitmap --------------
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

-- Materialized view để populate fact_sales_bitmap
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
