# scripts/ingest_csv.py
import os
import shutil
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import sys

from config_loader import load_config
from logger import setup_logger   # drop scripts. prefix if inside same folder


# Load config
config = load_config()
logger = setup_logger()

SOURCE_DIR = config["paths"]["source_csv"]
RAW_DATA_PATH = config["paths"]["raw_data"]
DB_PATH = config["paths"]["db_path"]

engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)


def ingest_csv_files():
    try:
        logger.info("Starting dynamic CSV ingestion...")

        os.makedirs(RAW_DATA_PATH, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


        for file in os.listdir(SOURCE_DIR):
            if file.lower().endswith(".csv"):
                src = os.path.join(SOURCE_DIR, file)
                dest = os.path.join(RAW_DATA_PATH, f"{timestamp}_{file}")
                shutil.copyfile(src, dest)
                logger.info(f"Ingested {file} → {dest}")

                # Load into Postgres
                table_name = file.replace(".csv", "")
                df = pd.read_csv(src, encoding="utf-8", low_memory=False)
                try:
                    df.to_sql(table_name, engine, if_exists="replace", index=False)
                    logger.info(f"Loaded {file} into Postgres table {table_name}")
                except Exception as e:
                    logger.error(f"Failed to load {file} into Postgres: {e}")

        # conn.close()
        logger.info("CSV ingestion completed successfully.")

    except Exception as e:
        logger.error(f"Error during CSV ingestion: {e}")

# NEW: wrap in a main() function
def main():
    ingest_csv_files()

    
if __name__ == "__main__":
    main()
    sys.stdout.flush()
    sys.stderr.flush()
