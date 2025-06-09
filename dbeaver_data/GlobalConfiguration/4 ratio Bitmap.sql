SELECT 
    orderID,
    orderDate,
    customerID,
    bitmapCardinality(groupBitmapMergeState(product_bitmap)) AS product_count,
    sumMerge(quantity) AS total_quantity,
    sumMerge(salesamount) AS total_sales
FROM fact_sales_bitmap
WHERE toYear(orderDate) = 2024
GROUP BY orderID, orderDate, customerID
LIMIT 10;






WITH target_product_ids AS (
    SELECT groupBitmapState(productID) as target_bitmap
    FROM dim_products
    WHERE productName IN ('Chips', 'Yogurt')
)
SELECT
     countIf(
            bitmapCardinality(
                bitmapAnd(
                    groupBitmapMergeState(product_bitmap),
                    (SELECT target_bitmap FROM target_product_ids)
                )
            ) = bitmapCardinality((SELECT target_bitmap FROM target_product_ids))
        ) / count()
     as ratio
FROM fact_sales_bitmap
WHERE toYear(orderDate) = 2025
GROUP BY orderID, orderDate, customerID
LIMIT 10;



WITH target_product_ids AS (
    SELECT groupBitmapState(productID) as target_bitmap
    FROM dim_products
    WHERE productName IN ('Chips', 'Yogurt')
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
WHERE toYear(orderDate) = 2025
GROUP BY orderID, orderDate, customerID
  );



-- bitmap
WITH target_product_ids AS (
    SELECT groupBitmapState(productID) as target_bitmap
    FROM dim_products
    WHERE productName IN ('Chips', 'Yogurt')
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
WHERE toYear(orderDate) = 2025
GROUP BY orderID, orderDate, customerID
  );