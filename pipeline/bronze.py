import datetime
import logging
from typing import Dict, Any

import polars as pl
from prefect import task
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import DB_CONNECTION_STRING, BRONZE_SCHEMA_NAME, SOURCE_TABLES

# Configure logger for bronze layer
logger = logging.getLogger(__name__)

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for PostgreSQL.
    """
    return create_engine(DB_CONNECTION_STRING)

@task(name="ingest_to_bronze", retries=3, retry_delay_seconds=10)
def ingest_table_to_bronze(table_name: str) -> Dict[str, Any]:
    """
    Ingests data from a source PostgreSQL table into the bronze schema.
    Adds an `ingestion_timestamp` column to the data.
    
    Args:
        table_name (str): The name of the table to ingest.

    Returns:
        Dict[str, Any]: A dictionary containing the table name, records ingested, and status.
    """
    engine = get_db_engine()
    ingestion_timestamp = datetime.datetime.now(datetime.timezone.utc)
    records_ingested = 0
    try:
        # Ensure the bronze schema exists
        with engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA_NAME}")
            conn.commit()
            logger.info(f"Ensured schema '{BRONZE_SCHEMA_NAME}' exists.")

        logger.info(f"Reading data from source table '{table_name}'...")
        # Use pl.read_database for efficient data extraction into Polars DataFrame
        df = pl.read_database(query=f"SELECT * FROM {table_name}", connection=engine)
        logger.info(f"Successfully read {len(df)} records from source table '{table_name}'.")

        # Add ingestion timestamp column
        df = df.with_columns(pl.lit(ingestion_timestamp).alias("ingestion_timestamp"))

        # Write to bronze schema. 'replace' ensures idempotency for daily runs.
        logger.info(f"Writing {len(df)} records to {BRONZE_SCHEMA_NAME}.{table_name} (if_table_exists='replace')...")
        df.write_database(
            table_name=table_name,
            connection=engine,
            schema=BRONZE_SCHEMA_NAME,
            if_table_exists='replace' # Replaces the table if it exists
        )
        records_ingested = len(df)
        logger.info(f"Successfully ingested {records_ingested} records into {BRONZE_SCHEMA_NAME}.{table_name}.")
        return {"table": table_name, "records_ingested": records_ingested, "status": "success"}
    except SQLAlchemyError as e:
        logger.error(f"Database error during ingestion for {table_name}: {e}", exc_info=True)
        raise
    except pl.ComputeError as e:
        logger.error(f"Polars computation error during ingestion for {table_name}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during ingestion for {table_name}: {e}", exc_info=True)
        raise

@task(name="run_bronze_layer", retries=0)
def run_bronze_layer() -> Dict[str, Any]:
    """
    Orchestrates the ingestion of all source tables into the bronze layer.
    """
    logger.info(f"Starting Bronze layer processing for tables: {SOURCE_TABLES}")
    results = []
    for table in SOURCE_TABLES:
        result = ingest_table_to_bronze.submit(table_name=table)
        results.append(result)
    
    # Wait for all tasks to complete and collect results
    final_results = [res.result() for res in results]
    
    success_count = sum(1 for res in final_results if res and res.get("status") == "success")
    total_records = sum(res.get("records_ingested", 0) for res in final_results if res and res.get("status") == "success")

    logger.info(f"Bronze layer completed. Successfully processed {success_count}/{len(SOURCE_TABLES)} tables. Total records ingested: {total_records}.")
    return {"status": "completed", "details": final_results, "total_records_ingested": total_records}
