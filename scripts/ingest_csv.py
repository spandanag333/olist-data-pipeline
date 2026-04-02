# scripts/ingest_csv.py
import os
import shutil
import sqlite3
import pandas as pd
from datetime import datetime

from config_loader import load_config
from logger import setup_logger   # drop scripts. prefix if inside same folder


# Load config
config = load_config()
logger = setup_logger()

SOURCE_DIR = config["paths"]["source_csv"]
RAW_DATA_PATH = config["paths"]["raw_data"]
DB_PATH = config["paths"]["db_path"]

def ingest_csv_files():
    try:
        logger.info("Starting dynamic CSV ingestion...")

        os.makedirs(RAW_DATA_PATH, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Connect to SQLite
        conn = sqlite3.connect(DB_PATH)

        for file in os.listdir(SOURCE_DIR):
            if file.lower().endswith(".csv"):
                src = os.path.join(SOURCE_DIR, file)
                dest = os.path.join(RAW_DATA_PATH, f"{timestamp}_{file}")
                shutil.copyfile(src, dest)
                logger.info(f"Ingested {file} → {dest}")

                # Load into SQLite
                table_name = file.replace(".csv", "")
                df = pd.read_csv(src)
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                logger.info(f"Loaded {file} into table {table_name}")

        conn.close()
        logger.info("CSV ingestion completed successfully.")

    except Exception as e:
        logger.error(f"Error during CSV ingestion: {e}")

if __name__ == "__main__":
    ingest_csv_files()
