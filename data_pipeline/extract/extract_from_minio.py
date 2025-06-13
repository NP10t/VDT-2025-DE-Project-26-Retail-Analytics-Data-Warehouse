# data_pipeline/data_extraction.py
from pyspark.sql import SparkSession
from minio.error import S3Error

def extract_from_minio(spark, bucket_name, object_name):
    """
    Extract data from a CSV file stored in MinIO and return a PySpark DataFrame.

    This function reads a CSV file from the specified MinIO bucket and object path,
    using Spark's S3-compatible interface. It includes header and infers schema.

    Args:
        spark (SparkSession): Initialized SparkSession with MinIO configurations.
        bucket_name (str): Name of the MinIO bucket.
        object_name (str): Path to the object in MinIO (e.g., 'raw/orders.csv').

    Returns:
        DataFrame: PySpark DataFrame containing the data from the CSV file.

    Raises:
        S3Error: If the file or bucket does not exist or there is a connection error.
        ValueError: If the input parameters are invalid.
    """
    # Validate inputs
    if not bucket_name or not object_name:
        raise ValueError("Bucket name and object name must not be empty.")

    try:
        # Construct MinIO S3 path
        input_path = f"s3a://{bucket_name}/{object_name}"

        # Read CSV file from MinIO
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

        # Check if DataFrame is empty
        if df.count() == 0:
            print(f"Warning: No data found in {input_path}.")
            return None

        # Log schema for debugging
        print(f"Extracted data from {input_path}. Schema:")
        df.printSchema()

        return df

    except S3Error as e:
        print(f"Error accessing MinIO at {input_path}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while extracting data from {input_path}: {e}")
        raise