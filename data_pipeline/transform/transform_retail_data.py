# data_pipeline/data_processing.py
from pyspark.sql.functions import col, regexp_replace
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_retail_data(df):
    """
    Transforms retail data: renames columns, converts orderID to numeric, and orderDate to DateType.

    Args:
        df: Input Spark DataFrame.

    Returns:
        Spark DataFrame after transformation.

    Raises:
        Exception: If the transformation fails.
    """

    
    try:
        return df.withColumnRenamed("orderdate", "orderDate") \
                .withColumn("orderID", regexp_replace(col("orderID"), "^ORD-", "").cast("integer"))
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise