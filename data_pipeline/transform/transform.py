# data_pipeline/data_processing.py
from pyspark.sql.functions import col, sum

def transform(df):
    """
    Transform raw data by cleaning and aggregating.

    This function cleans the salesamount column to ensure it contains valid double values
    and aggregates data by productName to compute total sales.

    Args:
        df (DataFrame): Input PySpark DataFrame from extract step.

    Returns:
        DataFrame: Transformed DataFrame with aggregated data.
    """
    if df is None:
        print("Input DataFrame is None, skipping transformation.")
        return None

    # Clean salesamount column
    df_clean = df.filter(col("salesamount").cast("double").isNotNull())
    df_clean = df_clean.withColumn("salesamount", col("salesamount").cast("double"))

    # Aggregate by productName
    sales_by_product = df_clean.groupBy("productName").agg(
        sum(col("salesamount")).alias("total_sales")
    ).orderBy("total_sales", ascending=False).limit(10)

    return sales_by_product