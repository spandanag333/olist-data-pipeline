# scripts/transform_data.py
import os
import glob
import pandas as pd
import json


from config_loader import load_config
from logger import setup_logger
from sqlalchemy import create_engine

# Database engine
engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)


# Load config + logger
config = load_config()
logger = setup_logger()

RAW_FOLDER = config["paths"]["raw_data"]
PROCESSED_DIR = config["paths"]["processed_data"]

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)


def clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Apply dynamic cleaning rules depending on file content.
    """
    try:
        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Convert date/time columns
        for col in df.columns:
            if any(keyword in col for keyword in ["date", "time", "timestamp"]):
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Attempt numeric conversion for object columns
        for col in df.select_dtypes(include=["object"]).columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="ignore")
            except Exception:
                pass

        # Fill missing values
        df = df.fillna({
            col: 0 if df[col].dtype.kind in "biufc" else "unknown"
            for col in df.columns
        })

        logger.info(f"Cleaned dataframe for {table_name} with {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Error cleaning {table_name}: {e}")
        return None


def convert_dicts_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert dict-like values to JSON strings for safe insertion into Postgres.
    """
    return df.applymap(lambda x: json.dumps(x) if isinstance(x, dict) else x)


def save_dataframe(df: pd.DataFrame, table_name: str):
    try:
        df.to_sql(f"{table_name}_clean", engine, if_exists="replace", index=False)
        logger.info(f"Saved cleaned data → Postgres table {table_name}_clean")
    except Exception as e:
        logger.error(f"Error saving {table_name}: {e}")


def get_business_tables():
    # Explicit list of Olist business tables
    return [
        "olist_customers_dataset",
        "olist_orders_dataset",
        "olist_order_items_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "olist_geolocation_dataset",
        "olist_order_reviews_dataset",
        "olist_order_payments_dataset",
        "product_category_name_translation"
    ]


def main():
    logger.info("Starting transformation pipeline...")
    errors = 0

    try:
        tables = get_business_tables()
        for table in tables:
            logger.info(f"Processing table {table} from Postgres")
            df = pd.read_sql(f"SELECT * FROM {table}", engine)

            df_clean = clean_dataframe(df, table)
            if df_clean is None:
                errors += 1
                continue

            # Convert dicts before saving
            df_clean = convert_dicts_to_json(df_clean)

            save_dataframe(df_clean, table)

        if errors > 0:
            logger.error(f"Transformation completed with {errors} errors.")
        else:
            logger.info("Transformation completed successfully.")

    except Exception as e:
        logger.error(f"Transformation pipeline failed: {e}")


if __name__ == "__main__":
    main()