# tests/test_data_extraction.py
import pytest
from pyspark.sql import SparkSession
from data_pipeline.utils.spark_utils import init_spark
from data_pipeline.utils.minio_utils import init_minio_client, upload_to_minio
from data_pipeline.data_extraction import extract_from_minio
import os

@pytest.fixture(scope="module")
def spark():
    """Fixture to provide a SparkSession for all tests."""
    spark = init_spark(app_name="TestDataExtraction")
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def minio_client():
    """Fixture to provide a MinIO client and bucket name."""
    client, bucket_name = init_minio_client()
    return client, bucket_name

@pytest.fixture(scope="module")
def setup_test_data(minio_client):
    """Fixture to upload a test CSV file to MinIO."""
    client, bucket_name = minio_client
    test_file = "data/test/test_orders.csv"
    
    # Create a sample CSV file with expected columns
    os.makedirs("data/test", exist_ok=True)
    with open(test_file, "w") as f:
        f.write("orderID,orderdate,productID,productName,customerID,quantity,salesamount\n")
        f.write("ORD-000001,2024-09-28,4,Cheese,b0ee254d-3280-480c-8d92-71740be49dde,3,272.37\n")
        f.write("ORD-000001,2024-09-28,49,Mushrooms,b0ee254d-3280-480c-8d92-71740be49dde,5,306.45\n")
    
    # Upload test file to MinIO under 'test' prefix
    upload_to_minio(client, bucket_name, test_file, prefix="test")
    return bucket_name, "test/test_orders.csv"

def test_dataframe_columns(spark, setup_test_data):
    """Test that the DataFrame extracted from MinIO has the expected columns."""
    bucket_name, object_name = setup_test_data
    
    # Extract DataFrame
    df = extract_from_minio(spark, bucket_name, object_name)
    
    # Expected columns based on orders.csv schema
    expected_columns = ["orderID", "orderdate", "productID", "productName", "customerID", "quantity", "salesamount"]
    
    # Get actual columns
    actual_columns = df.columns
    
    # Assert that the columns match
    assert actual_columns == expected_columns, f"Expected columns {expected_columns}, but got {actual_columns}"