python3 -m venv venv

linux
source venv/bin/activate

window
venv\data_pipeline\activate

python -m data_pipeline.etl.ingest

python data_pipeline\run_etl.py

pip install -r requirements.txt 

bash download_spark_dependencies.sh

bash run_etl.sh


docker exec -it clickhouse-server clickhouse-client --user admin --password admin123

docker exec -it clickhouse-server bash

apt-get update && apt-get install -y nano   
nano /etc/clickhouse-server/users.xml

docker exec -it clickhouse-server clickhouse-client
CREATE TABLE default.test_table (date String, value Int32) ENGINE = MergeTree() ORDER BY date;

SELECT * FROM system.materialized_views WHERE database = vdtdatabase AND name = 'gold_aggregates_mv';

INSERT INTO vdtdatabase.gold_aggregates
SELECT
    orderdate,
    productID,
    sumState(quantity) AS total_quantity,
    sumState(salesamount) AS total_salesamount
FROM vdtdatabase.gold
GROUP BY orderdate, productID;

SELECT
    orderdate,
    productID,
    sumMerge(total_quantity) AS total_quantity,
    sumMerge(total_salesamount) AS total_salesamount
FROM vdtdatabase.gold_aggregates
GROUP BY orderdate, productID
LIMIT 2;