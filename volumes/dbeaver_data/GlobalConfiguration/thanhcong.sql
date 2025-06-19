-- Chia nhỏ data và UNION ALL kết quả

WITH chunks AS (
    SELECT 
        sub_array,
        count(*) as frequency
    FROM (
        SELECT arrayJoin(
            arrayFilter( x -> length(x) >= 2 and length(x) <= 6,
                arrayMap(bit_mask -> arrayFilter( (x, i)->bitTest(bit_mask, i-1),  arr_id, arrayEnumerate(arr_id ) ), 
                          arrayEnumerate(range(1, toInt32(pow(2, length(arr_id))))))
            )
        ) AS sub_array
        FROM (
            SELECT DISTINCT arrayMap(x -> toInt64(x), splitByString('-', productGroupID)) AS arr_id
            FROM dim_product_group
            left join fact_sales on dim_product_group.productGroupKey = fact_sales.productGroupKey
            WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202401 and 202412
            -- and rowNumberInAllBlocks() % 1 = 0  -- Lấy 1/4 data
        )
    )
    GROUP BY sub_array
)
SELECT 
    sub_array,
    sum(frequency) as total_frequency
FROM chunks
GROUP BY sub_array
ORDER BY total_frequency DESC

CREATE TEMPORARY TABLE subset_frequencies (
    sub_array Array(Int64),
    total_frequency UInt64
) ENGINE = Memory;

INSERT INTO subset_frequencies
WITH chunks AS (
    SELECT 
        sub_array,
        count(*) AS frequency
    FROM (
        SELECT arrayJoin(
            arrayFilter(
                x -> length(x) >= 2 AND length(x) <= 6,
                arrayMap(
                    bit_mask -> arrayFilter(
                        (x, i) -> bitTest(bit_mask, i - 1),
                        arr_id,
                        arrayEnumerate(arr_id)
                    ),
                    arrayEnumerate(range(1, toInt32(pow(2, length(arr_id)))))
                )
            )
        ) AS sub_array
        FROM (
            SELECT arrayMap(x -> toInt64OrNull(x), splitByString('-', productGroupID)) AS arr_id
            FROM dim_product_group
            -- LEFT JOIN fact_sales ON dim_product_group.productGroupKey = fact_sales.productGroupKey
            -- WHERE toYYYYMM(fact_sales.orderDate) BETWEEN 202501 AND 202512
            WHERE rowNumberInAllBlocks() % 3 = 0
        )
    )
    GROUP BY sub_array
)
SELECT 
    sub_array,
    sum(frequency) AS total_frequency
FROM chunks
GROUP BY sub_array
ORDER BY total_frequency DESC;


select sum(total_frequency) as total_frequency
from subset_frequencies
group by sub_array
ORDER BY total_frequency DESC;

select * from subset_frequencies limit 5;

SELECT
    sub_array,
    sum(total_frequency) AS duplicate_count
FROM subset_frequencies
GROUP BY sub_array
HAVING count(*) >= 1