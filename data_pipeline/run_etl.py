# main.py
from data_pipeline import ( 
                           ingest_data_to_minio, 
                           extract_from_minio, 
                           transform_retail_data,
                           load_to_clickhouse,
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

    # Initialize Spark
    spark = init_spark()

    # Get MinIO bucket name
    _, bucket_name = init_minio_client()
    
    
    spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    spark.conf.set("spark.sql.catalog.clickhouse.host", os.getenv("CLICKHOUSE_HOST", "127.0.0.1"))
    spark.conf.set("spark.sql.catalog.clickhouse.protocol", os.getenv("CLICKHOUSE_PROTOCOL", "http"))
    spark.conf.set("spark.sql.catalog.clickhouse.http_port", os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    spark.conf.set("spark.sql.catalog.clickhouse.user", os.getenv("CLICKHOUSE_USER", "default"))
    spark.conf.set("spark.sql.catalog.clickhouse.password", os.getenv("CLICKHOUSE_PASSWORD", "123456"))
    spark.conf.set("spark.sql.catalog.clickhouse.database", os.getenv("CLICKHOUSE_DB", "default"))
    spark.conf.set("spark.clickhouse.write.format", os.getenv("CLICKHOUSE_WRITE_FORMAT", "json"))
    spark.conf.set("spark.clickhouse.option.http_version", "1.1")
    spark.conf.set("spark.clickhouse.option.http_client_impl", "apache4")

    logger.info(f"ClickHouse Host: {os.getenv('CLICKHOUSE_HOST')}")
    logger.info(f"ClickHouse User: {os.getenv('CLICKHOUSE_USER')}")
    logger.info(f"ClickHouse Database: {os.getenv('CLICKHOUSE_DB')}")
    
    
    data = [("2023-01-01", 1), ("2023-01-02", 2)]
    df = spark.createDataFrame(data, ["date", "value"])
    # spark.sql("CREATE TABLE clickhouse.vdtdatabase.test_table (date String, value Int32) ENGINE = MergeTree() ORDER BY date")
    df.writeTo("clickhouse.vdtdatabase.test_table").append()

    # Extract data from MinIO
    # df = extract_from_minio(spark, bucket_name, "raw/retail.csv")
    
    # df.show()  # Display the DataFrame for debugging

    # clickhouse_client = init_clickhouse_client()

    # # Create ClickHouse tables
    # create_clickhouse_tables(clickhouse_client)

    # # Transform data
    # dim_date, dim_product, dim_customer, fact_sales = transform_retail_data(df)

    # # Load data into ClickHouse
    # load_to_clickhouse(spark, dim_date, dim_product, dim_customer, fact_sales)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()