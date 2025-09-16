#!/bin/bash

# dos2unix normalize_data.sh  # convert line endings from Windows to Unix

set -e # Dừng script nếu bất kỳ lệnh nào thất bại

echo "▶️  Step 1: Uploading retails.csv to MinIO..."
# python collect_data/upload_data_to_minIO.py
# python -m data_pipeline.ingest.ingest


echo "✅  Step 1 complete."

echo "▶️  Step 2: Extracting, transforming, validating, and save as parquet file with Spark..."

# docker exec project_viettel-spark-master-1 spark-submit data_pipeline/run_etl.py

# docker exec project_viettel-spark-master-1 spark-submit \
#     --master spark://spark-master:7077 \
#     --deploy-mode client \
#     data_pipeline/run_etl.py

docker exec vdt-2025-de-project-26-retail-analytics-data-warehouse-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  data_pipeline/run_etl.py

echo "✅  Step 2 complete, processed data saved to MinIO as parquet file at vdt-data/cleaned_raw/retail_cleaned"