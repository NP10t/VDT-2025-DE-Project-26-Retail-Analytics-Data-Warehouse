import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
import os

def load_to_clickhouse(spark, dim_date, dim_product, dim_customer, fact_sales):
    """
    Load transformed DataFrames into ClickHouse using the Spark Connector.

    Args:
        spark (SparkSession): Initialized Spark session.
        dim_date (DataFrame): DataFrame containing dimension date data.
        dim_product (DataFrame): DataFrame containing dimension product data.
        dim_customer (DataFrame): DataFrame containing dimension customer data.
        fact_sales (DataFrame): DataFrame containing fact sales data.
    """
    try:

        # Configure Spark to use ClickHouse catalog
        catalog_name = "clickhouse"
        
        
        spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        spark.conf.set("spark.sql.catalog.clickhouse.host", os.getenv("CLICKHOUSE_HOST", "127.0.0.1"))
        spark.conf.set("spark.sql.catalog.clickhouse.protocol", os.getenv("CLICKHOUSE_PROTOCOL", "http"))
        spark.conf.set("spark.sql.catalog.clickhouse.http_port", os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
        spark.conf.set("spark.sql.catalog.clickhouse.user", os.getenv("CLICKHOUSE_USER", "default"))
        spark.conf.set("spark.sql.catalog.clickhouse.password", os.getenv("CLICKHOUSE_PASSWORD", "123456"))
        spark.conf.set("spark.sql.catalog.clickhouse.database", os.getenv("CLICKHOUSE_DB", "default"))
        spark.conf.set("spark.clickhouse.write.format", os.getenv("CLICKHOUSE_WRITE_FORMAT", "json"))
        spark.conf.set("spark.clickhouse.option.http_version", "1.1")

        logger.info(f"ClickHouse Host: {os.getenv('CLICKHOUSE_HOST')}")
        logger.info(f"ClickHouse User: {os.getenv('CLICKHOUSE_USER')}")
        logger.info(f"ClickHouse Database: {os.getenv('CLICKHOUSE_DB')}")
        
        logger.info(f"HTTP Version: {spark.conf.get('spark.clickhouse.option.http_version')}")

        # Define table mappings
        tables = [
            (dim_date, "dim_date"),
            (dim_product, "dim_product"),
            (dim_customer, "dim_customer"),
            (fact_sales, "fact_sales")
        ]

        # Write each DataFrame to ClickHouse
        for df, table_name in tables:
            logger.info(f"Writing DataFrame to ClickHouse table: {catalog_name}.vdtdatabase.{table_name}")
            df.writeTo(f"{catalog_name}.vdtdatabase.{table_name}").append()
            logger.info(f"Successfully wrote data to {table_name}")

    except Exception as e:
        logger.error(f"Failed to load data to ClickHouse: {e}")
        raise