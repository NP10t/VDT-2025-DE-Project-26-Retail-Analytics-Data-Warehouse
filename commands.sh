docker exec project_viettel-spark-master-1 spark-submit --master spark://spark-master:7077 --deploy-mode client data_pipeline/run_etl.py


clickhouse://admin:admin123@clickhouse-server:8123/vdt

docker exec -it superset_redis redis-cli -h localhost -p 6379 -n 1

INFO

KEYS superset_*

docker-compose logs superset | grep -i cache

docker exec -it superset_redis redis-cli -h localhost -p 6379 CONFIG GET maxmemory


# Gộp nhiều file và thực thi (wsl)
export $(cat .env | xargs)

(envsubst < star-schema/silver.sql; envsubst < star-schema/gold.sql; envsubst < star-schema/pre-aggregate.sql) | docker exec -i clickhouse-server clickhouse-client

(envsubst < star-schema/gold_itemset.sql;) | docker exec -i clickhouse-server clickhouse-client

(envsubst < star-schema/fp.sql;) | docker exec -i clickhouse-server clickhouse-client

(envsubst < star-schema/gold_fp.sql;) | docker exec -i clickhouse-server clickhouse-client


kiểm tra export chưa (wsl)
echo "CLICKHOUSE_DB: '$CLICKHOUSE_DB'"
echo "MINIO_ROOT_USER: '$MINIO_ROOT_USER'"
echo "MINIO_ROOT_PASSWORD: '$MINIO_ROOT_PASSWORD'"

-- Xóa và tạo lại
DROP NAMED COLLECTION IF EXISTS minio_config;

CREATE NAMED COLLECTION minio_config AS
    access_key_id = 'minioadmin',
    secret_access_key = 'minioadmin',
    url = 'http://minio:9000/';

-- Kiểm tra lại
SHOW CREATE NAMED COLLECTION minio_config;

SELECT * FROM s3(
    'http://minio:9000/vdt-data/cleaned_raw/retail_cleaned/*.parquet',
    'minioadmin',
    'minioadmin', 
    'Parquet'
) LIMIT 5;


echo "[$CLICKHOUSE_DB]" | od -c

dos2unix .env