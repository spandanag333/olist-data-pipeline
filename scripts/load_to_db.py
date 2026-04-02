# scripts/load_to_db.py
import pandas as pd
import sqlite3
import os

from config_loader import load_config

config = load_config()
SOURCE_DIR = config["paths"]["source_csv"]
DB_PATH = config["paths"]["db_path"]

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def load_all_csvs_to_db():
    conn = sqlite3.connect(DB_PATH)
    for file in os.listdir(SOURCE_DIR):
        if file.endswith(".csv"):
            table_name = file.replace(".csv", "")
            df = pd.read_csv(os.path.join(SOURCE_DIR, file))
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Loaded {file} into table {table_name}")
    conn.close()

if __name__ == "__main__":
    load_all_csvs_to_db()

