# main.py
from data_pipeline import ( 
                           ingest_data_to_minio, 
                           extract_from_minio, 
                           transform_retail_data,
                           load_to_minio_as_parquet,
                        )
from data_pipeline.utils.minio_utils import init_minio_client
from data_pipeline.utils.spark_utils import init_spark
from data_pipeline.utils.clickhouse_utils import (
    init_clickhouse_client,
    create_clickhouse_tables,
)

import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
import os



def main():
    """
    Main function to orchestrate data ingestion, extraction, transformation, and loading.
    """
    # Ingest data to MinIO
    ingest_data_to_minio(raw_data_dir="data/raw")

    # # Initialize Spark
    spark = init_spark()

    # # Get MinIO bucket name
    _, bucket_name = init_minio_client()

    # # Extract data from MinIO
    df = extract_from_minio(spark, bucket_name, "raw/retail.csv")
    
    # df.show()  # Display the DataFrame for debugging

    # clickhouse_client = init_clickhouse_client()

    # Create ClickHouse tables
    # create_clickhouse_tables(clickhouse_client)

    # Transform data
    cleaned_raw_df = transform_retail_data(df)
    
    load_to_minio_as_parquet(cleaned_raw_df, bucket_name, "retail_cleaned.parquet")

    # # # Load data into ClickHouse
    # # load_to_clickhouse(spark, dim_date, dim_product, dim_customer, fact_sales)

    # # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()