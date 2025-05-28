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
ORDER BY (orderID, customerID, orderdate);

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.customers
(
    customerID String,
    customerName String
)
ENGINE = MergeTree()
ORDER BY customerID;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.gold
(
    orderID String,
    orderdate Date,
    productID String,
    productName String,
    customerID String,
    customerName String,
    quantity Int32,
    salesamount Float64
)
ENGINE = MergeTree()
ORDER BY (orderdate, customerID, productID);

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.gold_mv
REFRESH EVERY 1 MINUTE
TO ${CLICKHOUSE_DB}.gold
AS
SELECT
    s.orderID,
    s.orderdate,
    s.productID,
    s.productName,
    s.customerID,
    c.customerName,
    s.quantity,
    s.salesamount
FROM ${CLICKHOUSE_DB}.silver AS s
LEFT JOIN ${CLICKHOUSE_DB}.customers AS c ON s.customerID = c.customerID;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.gold_aggregates
(
    orderdate Date,
    productID String,
    total_quantity AggregateFunction(sum, Int32),
    total_salesamount AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (orderdate, productID);

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}.gold_aggregates_mv
TO ${CLICKHOUSE_DB}.gold_aggregates
AS
SELECT
    orderdate,
    productID,
    sumState(quantity) AS total_quantity,
    sumState(salesamount) AS total_salesamount
FROM ${CLICKHOUSE_DB}.gold
GROUP BY orderdate, productID;