# E-commerce Medallion Pipeline

This project implements a daily batch data pipeline following the Medallion Architecture (Bronze, Silver, Gold layers) for e-commerce data. It ingests raw data from a PostgreSQL source, cleanses and joins it in the Silver layer, and aggregates it into analytics-ready tables in the Gold layer.

## Project Structure

```
ecommerce_medallion_pipeline/
├── main.py                     # Entry point to run the Prefect flow
├── config.py                   # Centralized configuration settings
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
├── pipeline/                   # Core pipeline logic for each medallion layer
│   ├── __init__.py
│   ├── bronze.py               # Ingestion from source to Bronze schema
│   ├── silver.py               # Cleaning, typing, joining Bronze to Silver schema
│   └── gold.py                 # Aggregation from Silver to Gold schema
└── flows/                      # Prefect flow definitions
    ├── __init__.py
    └── medallion_flow.py       # Defines the Prefect orchestration flow
```

## Stack

*   **Ingestion**: SQLAlchemy, psycopg2 (for PostgreSQL connectivity)
*   **Processing**: Polars (for efficient in-memory data transformations)
*   **Storage**: PostgreSQL (separate schemas for Bronze, Silver, Gold)
*   **Orchestration**: Prefect (for scheduling and managing pipeline tasks)

## Setup and Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd ecommerce_medallion_pipeline
    ```

2.  **Create a Python virtual environment** (recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: .\venv\Scripts\activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure PostgreSQL Connection**: 
    Edit `config.py` and set `DB_CONNECTION_STRING` to your PostgreSQL database connection details. Ensure the user has permissions to create schemas and tables.
    Example: `postgresql://user:password@host:5432/database_name`

## Running the Pipeline

To run the entire Medallion pipeline (Bronze -> Silver -> Gold) using Prefect:

1.  **Ensure your PostgreSQL database is running and accessible.**
2.  **Execute the `main.py` script**:
    ```bash
    python main.py
    ```

This will trigger the `ecommerce_medallion_pipeline_flow` defined in `flows/medallion_flow.py`, which orchestrates the tasks in `pipeline/bronze.py`, `pipeline/silver.py`, and `pipeline/gold.py`.

Prefect will log the progress of each task to the console. Upon successful completion, you should see the `bronze`, `silver`, and `gold` schemas populated with tables in your PostgreSQL database.

## Data Model (PostgreSQL Output)

### Bronze Schema
Raw data, preserving original structure, plus `ingestion_timestamp`.
*   `bronze.customers`
*   `bronze.orders`
*   `bronze.products`
*   `bronze.order_items`

### Silver Schema
Cleaned, type-casted, and joined data.
*   `silver.customers`
*   `silver.products`
*   `silver.order_items`
*   `silver.enriched_orders` (joined orders, customers, order_items, products data)

### Gold Schema
Aggregated, analytics-ready data.
*   `gold.daily_sales_summary` (Total sales and number of orders by date)
*   `gold.customer_lifetime_value` (Total amount spent by each customer)
*   `gold.product_performance` (Total quantity sold and revenue by product)

