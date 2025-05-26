# data_pipeline/data_processing.py
from pyspark.sql.functions import col, year, month, dayofmonth, quarter
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_retail_data(df):
    """
    Transform raw data into star schema format.
    """
    try:
        # Clean and preprocess data
        df = df.dropna()  # Remove rows with null values
        df = df.withColumn("orderdate", col("orderdate").cast("date"))
        df = df.withColumn("quantity", col("quantity").cast("int"))
        df = df.withColumn("salesamount", col("salesamount").cast("double"))

        # Create dim_date
        dim_date = df.select(
            col("orderdate").alias("date"),
            year("orderdate").alias("year"),
            month("orderdate").alias("month"),
            dayofmonth("orderdate").alias("day"),
            quarter("orderdate").alias("quarter")
        ).distinct()

        # Create dim_product
        dim_product = df.select(
            col("productID").alias("product_id"),
            col("productName").alias("product_name")
        ).distinct()

        # Create dim_customer (assuming customer_name is not available in raw data)
        dim_customer = df.select(
            col("customerID").alias("customer_id")
        ).distinct().withColumn("customer_name", col("customer_id"))

        # Create fact_sales
        fact_sales = df.select(
            col("orderID").alias("order_id"),
            col("orderdate").alias("order_date"),
            col("productID").alias("product_id"),
            col("customerID").alias("customer_id"),
            col("quantity"),
            col("salesamount").alias("sales_amount")
        )

        return dim_date, dim_product, dim_customer, fact_sales
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise