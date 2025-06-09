from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def validate_data(df: DataFrame) -> bool:
    """
    Validates dữ liệu Spark DataFrame trước khi load vào ClickHouse.

    Args:
        df: Spark DataFrame cần validate (cleaned_raw_df).

    Returns:
        bool: True nếu validation thành công, False nếu thất bại.
    """
    try:
        # 1. Kiểm tra sự tồn tại của các cột bắt buộc
        expected_columns = [
            "orderID", "orderDate", "productID", "productName",
            "customerID", "quantity", "salesamount"
        ]
        actual_columns = df.columns
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        if missing_columns:
            logger.error(f"Thiếu các cột bắt buộc: {missing_columns}")
            return False
        logger.info("Tất cả các cột bắt buộc đều tồn tại.")

        # 2. Kiểm tra kiểu dữ liệu
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
                logger.error(f"Cột {expected_field.name} có kiểu dữ liệu không khớp. Kỳ vọng: {expected_field.dataType}, Thực tế: {actual_field.dataType if actual_field else 'Không tồn tại'}")
                return False
        logger.info("Kiểu dữ liệu của tất cả các cột đều khớp.")

        # 3. Kiểm tra giá trị null
        for column in expected_columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                logger.error(f"Cột {column} chứa {null_count} giá trị null.")
                return False
        logger.info("Không có giá trị null trong các cột.")

        # 4. Kiểm tra quantity là số nguyên dương
        negative_quantity_count = df.filter(col("quantity") <= 0).count()
        if negative_quantity_count > 0:
            logger.error(f"Cột quantity chứa {negative_quantity_count} giá trị không dương.")
            return False
        logger.info("Tất cả giá trị trong cột quantity đều dương.")

        # 5. Kiểm tra salesamount không âm
        negative_salesamount_count = df.filter(col("salesamount") < 0).count()
        if negative_salesamount_count > 0:
            logger.error(f"Cột salesamount chứa {negative_salesamount_count} giá trị âm.")
            return False
        logger.info("Tất cả giá trị trong cột salesamount đều không âm.")
        
        # 6. Kiểm tra trùng lặp dựa trên tổ hợp khóa (orderID, orderDate, customerID, productID)
        key_columns = ["orderID", "orderDate", "customerID", "productID"]
        duplicate_count = df.groupBy(key_columns).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            logger.error(f"Tìm thấy {duplicate_count} bản ghi trùng lặp dựa trên {key_columns}.")
            return False
        logger.info("Không tìm thấy bản ghi trùng lặp.")

        logger.info("Validation dữ liệu thành công.")
        return True

    except Exception as e:
        logger.error(f"Lỗi trong quá trình validation: {str(e)}")
        return False