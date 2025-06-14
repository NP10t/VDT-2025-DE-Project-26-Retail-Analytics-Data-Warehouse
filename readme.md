## NGUYEN PHUC
## VIETTEL DIGITAL TAILENT 2025 PROJECT
## DATA WAREHOUSE FOR ASSOCIATION RULE MINING

![image](https://github.com/user-attachments/assets/8a8613a2-6f5f-48ea-ad05-43287b492546)

### Create Environment (for test optional)
``` bash
python3 -m venv venv
```
Linux:
``` bash
source venv/bin/activate
```
Windows:
``` bash
venv\Scripts\activate
```

### Initilize all the services
```bash
docker-compose up -d
```
Run spark with multiple worker:

```bash
docker-compose up --scale spark-worker=2
```
### Ingest to MinIO, Spark Extract, Transform, Validate, Load to Parquet files
``` bash
bash run_etl.sh
```
### Create Database
``` bash
envsubst < init.sql | docker exec -i clickhouse-server clickhouse-client
```
### Connect to ClickHouse
``` bash
docker exec -it clickhouse-server clickhouse-client --user <username> --password <password>
```
or
``` bash
docker exec -it clickhouse-server clickhouse-client
```
### Load Data from Parquet to Silver Layer at ClickHouse
``` sql
INSERT INTO silver
SELECT *
FROM s3(
    minio_config,
    url = 'http://minio:9000/vdt-data/cleaned_raw/retail_cleaned/*.parquet',
    format = 'Parquet'
);
```
Configuration
Add `<named_collection_control>1</named_collection_control>` into `configs/clickhouse-config/users.d`:
``` xml
<clickhouse>
  <!-- Docs: <https://clickhouse.com/docs/operations/settings/settings_users/> -->
  <users>
    <!-- Remove default user -->
    <default remove="remove">
    </default>
    <admin>
      <profile>default</profile>
      <networks>
        <ip>::/0</ip>
      </networks>
      <password><![CDATA[********]]></password>
      <quota>default</quota>
      <access_management>1</access_management>
      <named_collection_control>1</named_collection_control>
    </admin>
  </users>
</clickhouse>
```
