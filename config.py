from pathlib import Path

# --- Database Configuration ---
# PostgreSQL connection string
# Example: "postgresql://user:password@host:port/database"
# Ensure this user has permissions to create schemas and tables.
DB_CONNECTION_STRING: str = "postgresql://user:password@localhost:5432/ecommerce_db"

# Schema names for Medallion Architecture layers
BRONZE_SCHEMA_NAME: str = "bronze"
SILVER_SCHEMA_NAME: str = "silver"
GOLD_SCHEMA_NAME: str = "gold"

# Source table names in the raw database
SOURCE_TABLES: list[str] = [
    "customers",
    "orders",
    "products",
    "order_items"
]

# Bronze layer table names (typically same as source tables)
BRONZE_TABLE_NAMES: list[str] = [
    "customers",
    "orders",
    "products",
    "order_items"
]

# Silver layer table names
SILVER_TABLE_NAMES: list[str] = [
    "customers",
    "products",
    "order_items",
    "enriched_orders"
]

# Gold layer table names (aggregated reports)
GOLD_TABLE_NAMES: list[str] = [
    "daily_sales_summary",
    "customer_lifetime_value",
    "product_performance"
]

# --- Logging Configuration ---
# Define a base directory for logs if needed (not directly used by default Prefect logging)
LOG_BASE_DIR: Path = Path("./logs")
LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)
