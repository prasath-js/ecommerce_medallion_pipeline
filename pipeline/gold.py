import logging
from typing import Dict, Any

import polars as pl
from prefect import task
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import DB_CONNECTION_STRING, SILVER_SCHEMA_NAME, GOLD_SCHEMA_NAME

# Configure logger for gold layer
logger = logging.getLogger(__name__)

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for PostgreSQL.
    """
    return create_engine(DB_CONNECTION_STRING)

def create_gold_schema(engine) -> None:
    """
    Ensures the gold schema exists in the database.
    """
    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA_NAME}")
        conn.commit()
        logger.info(f"Ensured schema '{GOLD_SCHEMA_NAME}' exists.")

@task(name="calculate_daily_sales_summary", retries=3, retry_delay_seconds=10)
def calculate_daily_sales_summary() -> Dict[str, Any]:
    """
    Calculates daily sales summary from silver.enriched_orders and writes to gold.daily_sales_summary.
    """
    engine = get_db_engine()
    create_gold_schema(engine)
    target_table = "daily_sales_summary"
    records_processed = 0
    try:
        logger.info(f"Reading data from {SILVER_SCHEMA_NAME}.enriched_orders for daily sales summary...")
        df_enriched_orders = pl.read_database(query=f"SELECT * FROM {SILVER_SCHEMA_NAME}.enriched_orders", connection=engine)
        initial_rows = len(df_enriched_orders)
        logger.info(f"Read {initial_rows} records from silver.enriched_orders.")

        # Calculate daily_sales_summary: total sales by order_date
        df_summary = df_enriched_orders.group_by(pl.col("order_date").cast(pl.Date).alias("sale_date")).agg(
            pl.col("total_amount").sum().alias("total_sales"),
            pl.col("order_id").n_unique().alias("number_of_orders")
        ).sort("sale_date")

        records_processed = len(df_summary)
        logger.info(f"Writing {records_processed} records to {GOLD_SCHEMA_NAME}.{target_table} (if_table_exists='replace')...")
        df_summary.write_database(
            table_name=target_table,
            connection=engine,
            schema=GOLD_SCHEMA_NAME,
            if_table_exists='replace' # Replaces the table for idempotent daily refresh
        )
        logger.info(f"Successfully loaded {records_processed} records into gold.{target_table}.")
        return {"table": target_table, "records_processed": records_processed, "status": "success"}
    except SQLAlchemyError as e:
        logger.error(f"Database error calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise
    except pl.ComputeError as e:
        logger.error(f"Polars computation error calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise

@task(name="calculate_customer_lifetime_value", retries=3, retry_delay_seconds=10)
def calculate_customer_lifetime_value() -> Dict[str, Any]:
    """
    Calculates customer lifetime value from silver.enriched_orders and writes to gold.customer_lifetime_value.
    """
    engine = get_db_engine()
    create_gold_schema(engine)
    target_table = "customer_lifetime_value"
    records_processed = 0
    try:
        logger.info(f"Reading data from {SILVER_SCHEMA_NAME}.enriched_orders for customer lifetime value...")
        df_enriched_orders = pl.read_database(query=f"SELECT * FROM {SILVER_SCHEMA_NAME}.enriched_orders", connection=engine)
        initial_rows = len(df_enriched_orders)
        logger.info(f"Read {initial_rows} records from silver.enriched_orders.")

        # Calculate customer_lifetime_value: total spent by customer
        df_clv = df_enriched_orders.group_by("customer_id", "first_name", "last_name", "email").agg(
            pl.col("total_amount").sum().alias("total_spent")
        ).sort("customer_id")

        records_processed = len(df_clv)
        logger.info(f"Writing {records_processed} records to {GOLD_SCHEMA_NAME}.{target_table} (if_table_exists='replace')...")
        df_clv.write_database(
            table_name=target_table,
            connection=engine,
            schema=GOLD_SCHEMA_NAME,
            if_table_exists='replace'
        )
        logger.info(f"Successfully loaded {records_processed} records into gold.{target_table}.")
        return {"table": target_table, "records_processed": records_processed, "status": "success"}
    except SQLAlchemyError as e:
        logger.error(f"Database error calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise
    except pl.ComputeError as e:
        logger.error(f"Polars computation error calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise

@task(name="calculate_product_performance", retries=3, retry_delay_seconds=10)
def calculate_product_performance() -> Dict[str, Any]:
    """
    Calculates product sales quantity and revenue from silver.enriched_orders and writes to gold.product_performance.
    """
    engine = get_db_engine()
    create_gold_schema(engine)
    target_table = "product_performance"
    records_processed = 0
    try:
        logger.info(f"Reading data from {SILVER_SCHEMA_NAME}.enriched_orders for product performance...")
        df_enriched_orders = pl.read_database(query=f"SELECT * FROM {SILVER_SCHEMA_NAME}.enriched_orders", connection=engine)
        initial_rows = len(df_enriched_orders)
        logger.info(f"Read {initial_rows} records from silver.enriched_orders.")

        # Calculate product_performance: product sales quantity and revenue
        df_performance = df_enriched_orders.group_by("product_id", "product_name").agg(
            pl.col("quantity").sum().alias("total_quantity_sold"),
            (pl.col("quantity") * pl.col("price_at_purchase")).sum().alias("total_revenue")
        ).sort("product_id")

        records_processed = len(df_performance)
        logger.info(f"Writing {records_processed} records to {GOLD_SCHEMA_NAME}.{target_table} (if_table_exists='replace')...")
        df_performance.write_database(
            table_name=target_table,
            connection=engine,
            schema=GOLD_SCHEMA_NAME,
            if_table_exists='replace'
        )
        logger.info(f"Successfully loaded {records_processed} records into gold.{target_table}.")
        return {"table": target_table, "records_processed": records_processed, "status": "success"}
    except SQLAlchemyError as e:
        logger.error(f"Database error calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise
    except pl.ComputeError as e:
        logger.error(f"Polars computation error calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred calculating/loading gold.{target_table}: {e}", exc_info=True)
        raise

@task(name="run_gold_layer", retries=0)
def run_gold_layer() -> Dict[str, Any]:
    """
    Orchestrates the aggregation of silver data into the gold layer.
    """
    logger.info("Starting Gold layer processing...")
    results = []
    
    # Submit all gold tasks in parallel (or sequence if dependencies exist)
    results.append(calculate_daily_sales_summary.submit())
    results.append(calculate_customer_lifetime_value.submit())
    results.append(calculate_product_performance.submit())

    final_results = [res.result() for res in results]
    
    success_count = sum(1 for res in final_results if res and res.get("status") == "success")
    total_records = sum(res.get("records_processed", 0) for res in final_results if res and res.get("status") == "success")

    logger.info(f"Gold layer completed. Successfully processed {success_count}/{len(final_results)} reports. Total records generated: {total_records}.")
    return {"status": "completed", "details": final_results, "total_records_generated": total_records}
