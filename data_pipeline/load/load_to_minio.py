import os
import logging
from pyspark.sql import DataFrame

# Thiết lập logging
logger = logging.getLogger(__name__)

def load_to_minio_as_parquet(df: DataFrame, bucket_name: str, file_name: str, prefix: str = "cleaned_raw") -> None:
    """
    Load DataFrame to MinIO as Parquet files in a properly named directory.

    Args:
        df (DataFrame): The Spark DataFrame to load.
        bucket_name (str): The name of the MinIO bucket.
        file_name (str): The base name for the output directory (without .parquet extension).
        prefix (str, optional): The prefix path in the bucket. Defaults to "cleaned_raw".

    Raises:
        Exception: If writing to MinIO fails.
    """
    try:
        # Ensure file_name does not end with .parquet
        base_name = os.path.splitext(file_name)[0]  # Remove any .parquet extension
        # Construct the object path: prefix/base_name
        object_name = f"{prefix.rstrip('/')}/{base_name}"
        
        df.printSchema()  # Optional: Print schema for debugging

        # Save DataFrame to MinIO in Parquet format
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(f"s3a://{bucket_name}/{object_name}")
            

        logger.info(f"Data successfully loaded to MinIO bucket '{bucket_name}' at '{object_name}'")
    except Exception as e:
        logger.error(f"Failed to load data to MinIO at '{bucket_name}/{object_name}': {str(e)}")
        raise