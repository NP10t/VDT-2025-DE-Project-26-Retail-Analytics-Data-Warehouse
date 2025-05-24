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
