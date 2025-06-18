WITH order_products AS (
    SELECT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
)
select length(combo) combo_size, combo
from
( select a.orderID orderID1,
        b.orderID orderID2,
        groupArray(productName) share_prd_name,
        arrayEnumerate(share_prd_name) arr_enum,
        count(*) share_cnt,
        arrayJoin(range(1, toInt32(pow(2, share_cnt))  ) ) as mask_idx,
        arrayFilter(
          (x, i) -> bitTest(mask_idx, i - 1),
          share_prd_name,
          arr_enum
        ) as combo
FROM order_products a
join order_products b on a.productID = b.productID and a.orderID < b.orderID
join dim_products dp on a.productID = dp.productID
group by a.orderID, b.orderID )
where combo_size > 2
group by combo
order by combo_size
limit 20


---------

WITH order_products AS (
    SELECT 
        fs.orderID, 
        arraySort(groupArray(dp.productName)) as products
    FROM fact_sales fs
    JOIN dim_products dp ON fs.productID = dp.productID
    WHERE toYYYYMM(fs.orderDate) = 202405
    GROUP BY fs.orderID
    HAVING length(products) >= 2
)
SELECT 
    combo_size,
    combo,
    count(*) as support_count,
    count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
FROM (
    SELECT 
        orderID,
        length(combo) as combo_size,
        combo
    FROM order_products
    ARRAY JOIN 
        arrayFilter(x -> length(x) >= 2, 
            arrayMap(i -> 
                arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                range(1, toUInt32(pow(2, length(products))))
            )
        ) as combo
) 
GROUP BY combo_size, combo
HAVING support_count >= 2  -- Xuất hiện ít nhất 2 lần
ORDER BY support_count DESC, combo_size DESC
LIMIT 20;


---- debug ----------
WITH order_products AS (
    SELECT orderID, productID
    FROM fact_sales
    WHERE toYYYYMM(orderDate) = 202405
)
select length(combo) combo_size, combo, count(combo) frequency
from ( 
  select a.orderID orderID1,
          b.orderID orderID2,
          groupArray(productName) share_prd_name,
          arrayEnumerate(share_prd_name) arr_enum,
          count(*) share_cnt,
          arrayJoin(range(1, toInt32(pow(2, share_cnt))  ) ) as mask_idx,
          arrayFilter(
            (x, i) -> bitTest(mask_idx, i - 1),
            share_prd_name,
            arr_enum
          ) as combo
  FROM order_products a
  join order_products b on a.productID = b.productID and a.orderID < b.orderID
  join dim_products dp on a.productID = dp.productID
  group by a.orderID, b.orderID 
)
where combo_size > 0
group by combo
order by combo_size
limit 20






WITH order_products AS (
    SELECT 
        fs.orderID, 
        arraySort(groupArray(dp.productName)) as products
    FROM fact_sales fs
    JOIN dim_products dp ON fs.productID = dp.productID
    WHERE toYYYYMM(fs.orderDate) = 202405
    GROUP BY fs.orderID
    HAVING length(products) >= 2
)
    SELECT 
        length(combo) as combo_size,
        combo,
        count(*) as support_count,
        count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
    FROM order_products
    ARRAY JOIN 
        arrayFilter(x -> length(x) >= 2, 
                    arrayMap(i -> 
                            arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                            range(1, toUInt32(pow(2, length(products))))
                            )
                    ) as combo
GROUP BY combo_size, combo
HAVING support_count >= 2  -- Xuất hiện ít nhất 2 lần
ORDER BY support_count DESC, combo_size DESC
LIMIT 20;


--------- Aggregating Merge Tree-----------
WITH order_products AS (
    SELECT 
      arraySort(groupArrayMerge(product_set)) products
    FROM order_product_sets_3
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    group by orderID
    having countMerge(product_count) >= 2
)
    SELECT 
        length(combo) as combo_size,
        combo,
        count(*) as support_count,
        count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
    FROM order_products
    ARRAY JOIN 
        arrayFilter(x -> length(x) >= 2, 
            arrayMap(i -> 
                arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                range(1, toUInt32(pow(2, length(products))))
            )
        ) as combo
GROUP BY combo_size, combo
HAVING support_count >= 2 
ORDER BY support_count DESC, combo_size DESC
LIMIT 5;

--------- Replacing Merge Tree------------
WITH order_products AS (
    SELECT 
      arraySort(product_set) products
    FROM order_product_sets_2
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    having product_count >= 2
)
    SELECT 
        length(combo) as combo_size,
        combo,
        count(*) as support_count,
        count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
    FROM order_products
    ARRAY JOIN 
        arrayFilter(x -> length(x) >= 2, 
            arrayMap(i -> 
                arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                range(1, toUInt32(pow(2, length(products))))
            )
        ) as combo
GROUP BY combo_size, combo
HAVING support_count >= 2
ORDER BY support_count DESC, combo_size DESC
LIMIT 5;

---------  Trích orderID và Array Products từ Fact Sales -----------
WITH order_products AS (
    SELECT 
        fs.orderID, 
        arraySort(groupArray(productID)) as products
    FROM fact_sales fs
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    GROUP BY fs.orderID
    HAVING length(products) >= 2
)
    SELECT 
        length(combo) as combo_size,
        combo,
        count(*) as support_count,
        count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
    FROM order_products
    ARRAY JOIN 
        arrayFilter(x -> length(x) >= 2, 
            arrayMap(i -> 
                arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                range(1, toUInt32(pow(2, length(products))))
            )
        ) as combo
GROUP BY combo_size, combo
HAVING support_count >= 2 
ORDER BY support_count DESC, combo_size DESC
LIMIT 5;



WITH order_products AS (
    -- Chọn ra orderID và mảng các productID thuộc vè Order đó
    SELECT 
        fs.orderID, 
        arraySort(groupArray(productID)) as products
    FROM fact_sales fs
    where toYYYYMM(orderDate) BETWEEN 202405 and 202408
    GROUP BY fs.orderID
    HAVING length(products) >= 2 -- Chỉ quan tâm các Order có ít nhất 2 sản phẩm
)
    SELECT 
        length(combo) as combo_size,
        combo,
        count(*) as support_count, -- Số lần Combo xuất hiện
        count(*) * 100.0 / (SELECT count(*) FROM order_products) as support_percentage
    FROM order_products
    ARRAY JOIN -- Tạo ra tất cả tập hợp con từ danh sách sản phẩm thuộc về mỗi Order
        arrayFilter(x -> length(x) >= 2, 
            arrayMap(i -> 
                arrayFilter((val, idx) -> bitTest(i, idx-1), products, range(1, length(products)+1)),
                range(1, toUInt32(pow(2, length(products))))
            )
        ) as combo
GROUP BY combo_size, combo -- Gom nhóm theo Combo để đếm số Order chứa Combo đos
HAVING support_count >= 2  -- Combo phải xuất hiện ít nhất 2 lần
ORDER BY support_count DESC, combo_size DESC
LIMIT 5;



with orders as (SELECT 
count(*) as item_cnt,
groupArray(productID) all_items
FROM silver
-- where toYYYYMM(orderDate) = 202405
group by orderID
)
select 
  length(all_itemset) itemset_size,
  all_itemset
from orders
ARRAY JOIN arrayFilter(
  x -> length(x) < 7,
  arrayMap(
    bit_mask -> arrayFilter(
          (x, i) -> bitTest(bit_mask, i-1),
          all_items,
          arrayEnumerate(all_items)
        ),
    range(1, toInt32(pow(2, item_cnt)))
  )
) as all_itemset






with orders as (SELECT 
count(*) as item_cnt,
groupArray(productID) all_items
FROM silver
where toYYYYMM(orderDate) = 202405
group by orderID
),
  itemsets as (
  select 
    length(itemset) itemset_size,
    SHA256(arrayStringConcat(arraySort(itemset), '|')) as itemset_id
  from orders
  ARRAY JOIN arrayFilter(
    x -> length(x) < 6 and length(x) >= 2,
    arrayMap(
      bit_mask -> arrayFilter(
            (x, i) -> bitTest(bit_mask, i-1),
            all_items,
            arrayEnumerate(all_items)
          ),
      range(1, toInt32(pow(2, item_cnt)))
    )
  ) as itemset
), frequency_id as ( select count(*) frequency, itemset_id
from itemsets
where itemset_size = 3
group by itemset_id
) select * 
from frequency_id f
join itemsets i on f.itemset_id = i.itemset_id
order by frequency desc




