# data_pipeline/utils/spark_utils.py
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

def init_spark(app_name="RestaurantDataProcessing"):
    """
    Khởi tạo SparkSession với cấu hình MinIO và các JAR cần thiết.

    Args:
        app_name (str): Tên ứng dụng Spark. Mặc định là "RestaurantDataProcessing".

    Returns:
        SparkSession: Phiên Spark đã được cấu hình.

    Raises:
        ValueError: Nếu thiếu biến môi trường MINIO_ROOT_USER hoặc MINIO_ROOT_PASSWORD.
    """
    # Load biến môi trường từ .env
    load_dotenv()

    # Kiểm tra biến môi trường cần thiết
    required_vars = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Thiếu các biến môi trường: {', '.join(missing_vars)}")

    # Khởi tạo SparkSession với các JAR
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.533.jar,/opt/bitnami/spark/jars/postgresql-42.7.3.jar")
    spark = builder.getOrCreate()
    sc = spark.sparkContext

    # Cấu hình MinIO
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint.region", "ap-southeast-1")

    return spark