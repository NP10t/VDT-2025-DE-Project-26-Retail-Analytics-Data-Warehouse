# main.py
from data_pipeline import ( 
                           ingest_data_to_minio, 
                           extract_from_minio, 
                        )
from data_pipeline.utils.minio_utils import init_minio_client
from data_pipeline.utils.spark_utils import init_spark


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

    # Extract data from MinIO
    df = extract_from_minio(spark, bucket_name, "raw/retail.csv")
    
    df.show()  # Display the DataFrame for debugging

    # # Transform data
    # transformed_df = transform_data(df)

    # # Load transformed data to MinIO
    # load_to_minio(spark, transformed_df, bucket_name, "processed/sales_by_product.parquet")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()