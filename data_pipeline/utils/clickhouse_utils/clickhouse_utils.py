from clickhouse_driver import Client
import logging
import os
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_clickhouse_client():
    """
    Initialize a ClickHouse client from a container in the same Docker network.

    Returns:
        Client: A connected ClickHouse client object.

    Raises:
        Exception: If the connection fails due to configuration or network issues.
    """
    try:
        # Retrieve and validate environment variables
        user = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD', '')
        database = os.getenv('CLICKHOUSE_DB', 'default')
        clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse-server')

        # Log environment variable values for debugging
        logger.debug("Initializing ClickHouse client...")
        logger.debug(f"CLICKHOUSE_USER: {user}")
        logger.debug(f"CLICKHOUSE_PASSWORD: {'[empty]' if not password else password}")
        logger.debug(f"CLICKHOUSE_DB: {database}")

        # Warn if environment variables are not set
        if user == 'default':
            logger.warning("CLICKHOUSE_USER not set, using default: 'default'")
        if not password:
            logger.warning("CLICKHOUSE_PASSWORD not set, using empty password")
        if database == 'default':
            logger.warning("CLICKHOUSE_DB not set, using default database: 'default'")

        # Initialize ClickHouse client
        client = Client(
            host=clickhouse_host,
            port=9000,
            user=user,
            password=password,
            database=database
        )

        # Verify connection with a simple query
        client.execute('SELECT 1')
        logger.info("Connected successfully to ClickHouse, database: %s", database)
        return client

    except Exception as e:
        logger.error("Failed to connect to ClickHouse", str(e))
        raise

def create_clickhouse_tables(client):
    """
    Create ClickHouse tables for the star schema.
    """
    try:
        
        client.execute("""
        CREATE TABLE IF NOT EXISTS bronze
        (
            orderID String,
            orderdate Date,
            productID Int32,
            productName String,
            customerID String,
            quantity Int32,
            salesamount Float64
        )
        ENGINE = S3(
            'http://minio:9000/vdt-data/cleaned_raw/retail_cleaned/*.parquet',
            'minioadmin',
            'minioadmin',
            'Parquet'
        );
        """)
        
        # Create dim_date
        client.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date Date,
            year UInt16,
            month UInt8,
            day UInt8,
            quarter UInt8
        ) ENGINE = MergeTree()
        ORDER BY date
        """)

        # Create dim_product
        client.execute("""
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id String,
            product_name String
        ) ENGINE = MergeTree()
        ORDER BY product_id
        """)

        # Create dim_customer
        client.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id String,
            customer_name String DEFAULT ''
        ) ENGINE = MergeTree()
        ORDER BY customer_id
        """)

        # Create fact_sales
        client.execute("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            order_id String,
            order_date Date,
            product_id String,
            customer_id String,
            quantity UInt32,
            sales_amount Float64
        ) ENGINE = MergeTree()
        ORDER BY (order_date, order_id)
        """)
        logger.info("ClickHouse tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create ClickHouse tables: {e}")
        raise

