import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_clickhouse(client, dim_date, dim_product, dim_customer, fact_sales):
    """
    Load transformed data into ClickHouse.
    """
    try:
        # Convert Spark DataFrames to Pandas for ClickHouse insertion
        dim_date_pd = dim_date.toPandas()
        dim_product_pd = dim_product.toPandas()
        dim_customer_pd = dim_customer.toPandas()
        fact_sales_pd = fact_sales.toPandas()

        # Load dim_date
        if not dim_date_pd.empty:
            client.execute(
                "INSERT INTO dim_date (date, year, month, day, quarter) VALUES",
                dim_date_pd.to_dict('records')
            )
            logger.info("Loaded dim_date")

        # Load dim_product
        if not dim_product_pd.empty:
            client.execute(
                "INSERT INTO dim_product (product_id, product_name) VALUES",
                dim_product_pd.to_dict('records')
            )
            logger.info("Loaded dim_product")

        # Load dim_customer
        if not dim_customer_pd.empty:
            client.execute(
                "INSERT INTO dim_customer (customer_id, customer_name) VALUES",
                dim_customer_pd.to_dict('records')
            )
            logger.info("Loaded dim_customer")

        # Load fact_sales
        if not fact_sales_pd.empty:
            client.execute(
                "INSERT INTO fact_sales (order_id, order_date, product_id, customer_id, quantity, sales_amount) VALUES",
                fact_sales_pd.to_dict('records')
            )
            logger.info("Loaded fact_sales")
    except Exception as e:
        logger.error(f"Failed to load data into ClickHouse: {e}")
        raise