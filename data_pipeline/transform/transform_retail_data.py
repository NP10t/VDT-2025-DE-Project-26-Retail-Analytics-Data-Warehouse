# data_pipeline/data_processing.py
from pyspark.sql.functions import col, regexp_replace
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_retail_data(df):
    """
    Transform dữ liệu bán lẻ: đổi tên cột, chuyển đổi orderID thành số, và orderDate sang DateType.

    Args:
        df: Spark DataFrame đầu vào.

    Returns:
        Spark DataFrame đã được transform.

    Raises:
        Exception: Nếu transform thất bại.
    """
    
    try:
        return df.withColumnRenamed("orderdate", "orderDate") \
                .withColumn("orderID", regexp_replace(col("orderID"), "^ORD-", "").cast("integer"))
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise