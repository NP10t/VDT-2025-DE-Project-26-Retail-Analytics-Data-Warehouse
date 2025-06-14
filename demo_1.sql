select count(*) from fact_sales
-- 500.000

select count(*) from silver
-- 500.000

select count(orderID) 
from fact_sales 
where toYYYYMM(orderDate) = 202405
-- 1 row in set. Elapsed: 0.077 sec. Processed 12.35 thousand rows, 74.12 KB (160.37 thousand rows/s., 962.25 KB/s.)
-- Peak memory usage: 80.09 KiB.

select count(orderID) 
from silver 
where toYYYYMM(orderDate) = 202405
-- 1 row in set. Elapsed: 0.054 sec. Processed 500.00 thousand rows, 3.00 MB (9.24 million rows/s., 55.44 MB/s.)
-- Peak memory usage: 109.05 KiB.
