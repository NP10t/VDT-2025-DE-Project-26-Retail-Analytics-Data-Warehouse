#!/bin/bash

# dos2unix normalize_data.sh  # convert line endings from Windows to Unix

set -e # Dừng script nếu bất kỳ lệnh nào thất bại

# echo "▶️  Step 1: Uploading data to datalake..."
# python collect_data/upload_data_to_minIO.py
# python -m data_pipeline.ingest.ingest


echo "✅  Step 1 complete."

echo "▶️  Step 2: Loading processed to processed_data folder and data of the images table to the images_table_data folder"

# docker exec project_viettel-spark-master-1 spark-submit data_pipeline/run_etl.py

# docker exec project_viettel-spark-master-1 spark-submit \
#     --master spark://spark-master:7077 \
#     --deploy-mode client \
#     data_pipeline/run_etl.py

docker exec project_viettel-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  data_pipeline/run_etl.py

echo "✅  Step 2 complete, go to the processed_data and images_table_data folders to view the data."