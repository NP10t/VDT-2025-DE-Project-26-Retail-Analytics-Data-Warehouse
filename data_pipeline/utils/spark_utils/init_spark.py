# data_pipeline/utils/spark_utils.py
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

def init_spark(app_name="Project-26-VDT-2025-Data-Engineering"):
    """
        Initialize a SparkSession with configurations for MinIO and ClickHouse dependencies.

        Args:
            app_name (str): The name of the Spark application. Default is "Project-26-VDT-2025-Data-Engineering".

        Returns:
            SparkSession: A configured Spark session.

        Raises:
            ValueError: If the environment variables MINIO_ROOT_USER or MINIO_ROOT_PASSWORD are missing.
    """

    # Load environment variables from .env file
    load_dotenv()

    # Check for required environment variables
    required_vars = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Thiếu các biến môi trường: {', '.join(missing_vars)}")

    # Dependencies for ClickHouse
    # https://clickhouse.com/docs/integrations/apache-spark/spark-native-connector
    jars = [
        "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.533.jar",
        "/opt/bitnami/spark/jars/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar",

        "/opt/bitnami/spark/jars/clickhouse-jdbc-0.6.3.jar",
        "/opt/bitnami/spark/jars/httpclient-4.5.13.jar",
        "/opt/bitnami/spark/jars/httpcore-4.4.13.jar",
    ]

    # Initialize SparkSession with the specified application name and JARs
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", ",".join(jars))

    spark = builder.getOrCreate()
    sc = spark.sparkContext

    # Set configurations for MinIO
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint.region", "ap-southeast-1")

    return spark