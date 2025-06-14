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