python3 -m venv venv

linux
source venv/bin/activate

window
venv\Scripts\activate

python -m scripts.etl.ingest

python scripts\run_etl.py

pip install -r requirements.txt 

bash download_spark_dependencies.sh

bash run_etl.sh


docker exec -it clickhouse-server clickhouse-client --user admin --password admin123
