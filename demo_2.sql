select sum(satisfied) * 100 /count() from
(
select hasAll(groupArrayMerge(product_set), 
                ( select groupArray(productID) as ids 
                from dim_products 
                where productName in ['Cheese', 'Eggs']  ) as xx
              ) satisfied
from order_product_sets_3
where toYYYYMM(orderDate) = 202405
group by orderID
)
-- 1 row in set. Elapsed: 0.040 sec. Processed 2.34 thousand rows, 
-- 154.65 KB (58.57 thousand rows/s., 3.87 MB/s.)
-- Peak memory usage: 252.07 KiB.


select countIf(matched_item_cnt = 2) * 100.0 / (
                  select count(distinct orderID) 
                  from fact_sales 
                  where toYYYYMM(orderDate) = 202405
                  )
from
(select count(*) as matched_item_cnt
from fact_sales fs
join dim_products dp on fs.productID = dp.productID
where toYYYYMM(orderDate) = 202405
and dp.productName In ('Cheese', 'Eggs')
group by fs.orderID ) as groupOrder
-- 1 row in set. Elapsed: 0.046 sec. Processed 24.81 thousand rows, 
-- 199.85 KB (541.38 thousand rows/s., 4.36 MB/s.)
-- Peak memory usage: 270.37 KiB.