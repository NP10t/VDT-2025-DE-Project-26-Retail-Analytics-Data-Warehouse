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
ORDER BY (orderID, productID, orderDate, customerID);