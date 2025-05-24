docker exec -it clickhouse-server clickhouse-client --user admin --password admin123

python3 -m venv venv

linux
source venv/bin/activate

window
venv\Scripts\activate

python scripts\data_ingestion\upload_to_minio.py

pip install -r requirements.txt 