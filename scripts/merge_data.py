# scripts/merge_data.py
import os
import sqlite3
import pandas as pd

from config_loader import load_config
from logger import setup_logger

# Load config + logger
config = load_config()
logger = setup_logger()

DB_PATH = config["paths"]["db_path"]
MERGED_DIR = config["paths"]["merged_data"]
os.makedirs(MERGED_DIR, exist_ok=True)

def build_merge_query():
    """Build SQL join query dynamically from config.yaml."""
    fact_table = config["fact_table"]
    relationships = config.get("relationships", {})

    query = f"SELECT * FROM {fact_table} f"
    for dim_name, rel in relationships.items():
        query += (
            f" LEFT JOIN {dim_name} d_{dim_name}"
            f" ON f.{rel['fact_key']} = d_{dim_name}.{rel['dim_key']}"
        )
    return query

def merge_tables():
    conn = sqlite3.connect(DB_PATH)
    fact_table = config["fact_table"]
    relationships = config.get("relationships", {})

    merged_df = pd.read_sql_query(f"SELECT * FROM {fact_table}", conn)
    logger.info(f"Fact table {fact_table} → {len(merged_df):,} rows")

    for dim_name, rel in relationships.items():
        # Get dimension columns
        dim_cols = pd.read_sql_query(f"PRAGMA table_info({dim_name})", conn)["name"].tolist()

        # Build SELECT with prefixed dimension columns
        dim_select = ", ".join([f"d.{c} AS {dim_name}_{c}" for c in dim_cols])

        join_query = (
            f"SELECT f.*, {dim_select} "
            f"FROM {fact_table} f "
            f"LEFT JOIN {dim_name} d "
            f"ON f.{rel['fact_key']} = d.{rel['dim_key']}"
        )

        merged_df = pd.read_sql_query(join_query, conn)
        logger.info(
            f"Orders merged with {dim_name} "
            f"on {rel['fact_key']}={rel['dim_key']} → {len(merged_df):,} rows"
        )

    conn.close()
    return merged_df

def save_fact_table(df):
    out_path_csv = os.path.join(MERGED_DIR, "fact_orders.csv")
    out_path_parquet = os.path.join(MERGED_DIR, "fact_orders.parquet")

    df.to_csv(out_path_csv, index=False)
    df.to_parquet(out_path_parquet, index=False)

    logger.info(f"Saved merged fact table → {out_path_csv} and {out_path_parquet}")


def main():
    logger.info("Starting SQL‑based merge pipeline...")
    fact_df = merge_tables()
    if fact_df is not None:
        save_fact_table(fact_df)
        logger.info("Merge pipeline completed successfully.")
    else:
        logger.error("Merge pipeline failed.")

if __name__ == "__main__":
    main()
