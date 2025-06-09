# data_pipeline/data_processing.py
from pyspark.sql.functions import col, year, month, dayofmonth, quarter
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_retail_data(df):
    try:
        return df
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise