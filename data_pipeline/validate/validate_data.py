from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def validate_data(df: DataFrame) -> bool:
    """
        Validates the Spark DataFrame before loading it into ClickHouse.

        Args:
            df: The Spark DataFrame to validate (cleaned_raw_df).

        Returns:
            bool: True if validation is successful, False otherwise.
    """

    try:
        # 1. Check for required columns
        expected_columns = [
            "orderID", "orderDate", "productID", "productName",
            "customerID", "quantity", "salesamount"
        ]
        actual_columns = df.columns
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        logger.info("All required columns are present.")

        # 2. Check data types of each column
        expected_schema = StructType([
            StructField("orderID", IntegerType(), False),
            StructField("orderDate", DateType(), False),
            StructField("productID", IntegerType(), False),
            StructField("productName", StringType(), False),
            StructField("customerID", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("salesamount", DoubleType(), False)
        ])
        actual_schema = df.schema
        for expected_field in expected_schema:
            actual_field = next((f for f in actual_schema if f.name == expected_field.name), None)
            if not actual_field or actual_field.dataType != expected_field.dataType:
                logger.error(f"Col {expected_field.name} has un-expected type. Expect: {expected_field.dataType}, Thực tế: {actual_field.dataType if actual_field else 'Không tồn tại'}")
                return False
        logger.info("All columns have the expected data types.")

        # 3. Check for null values in required columns
        for column in expected_columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                logger.error(f"col {column} has {null_count} null values.")
                return False
        logger.info("The DataFrame does not contain any null values in required columns.")

        # 4. Check quantity is positive
        negative_quantity_count = df.filter(col("quantity") <= 0).count()
        if negative_quantity_count > 0:
            logger.error(f"Col quantity has {negative_quantity_count} non-positive values.")
            return False
        logger.info("All values in col quantity are positive.")

        # 5. Check salesamount is non-negative
        negative_salesamount_count = df.filter(col("salesamount") < 0).count()
        if negative_salesamount_count > 0:
            logger.error(f"Col salesamount has {negative_salesamount_count} non-positive values.")
            return False
        logger.info("All values in col salesamount are non-negative.")
        
        # 6. check for duplication on (orderID, orderDate, customerID, productID)
        key_columns = ["orderID", "orderDate", "customerID", "productID"]
        duplicate_count = df.groupBy(key_columns).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            logger.error(f"Found {duplicate_count} duplicated records based on {key_columns}.")
            return False
        logger.info("No duplicate records found based on the key columns.")

        logger.info("Validation successful: DataFrame is ready for loading into ClickHouse.")
        return True

    except Exception as e:
        logger.error("Exception occurred during validation: {str(e)}")
        return False