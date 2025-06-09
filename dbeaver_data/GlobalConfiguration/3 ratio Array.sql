with target_ids as (
  SELECT groupArray(productID)
  from dim_products
  WHERE productName in ('Chips', 'Yogurt')
)
  SELECT countIf(hasAll(product_set, (select * from target_ids) )) / count() 
  from order_product_sets_2
  WHERE year(orderDate) == 2025;