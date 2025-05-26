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