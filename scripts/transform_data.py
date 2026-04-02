# scripts/transform_data.py
import os
import glob
import pandas as pd

from config_loader import load_config
from logger import setup_logger


# Load config + logger
config = load_config()
logger = setup_logger()

RAW_FOLDER = config["paths"]["raw_data"]
PROCESSED_DIR = config["paths"]["processed_data"]

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

def get_latest_files():
    """
    Get the latest ingested CSVs (timestamped) from raw folder.
    Returns a dict mapping original filename -> latest file path.
    """
    files = glob.glob(os.path.join(RAW_FOLDER, "*.csv"))
    if not files:
        raise FileNotFoundError("No CSV files found in raw folder")

    latest_files = {}
    for f in files:
        # Remove timestamp prefix (everything before first underscore)
        base = os.path.basename(f).split("_", 1)[-1]
        # Keep the most recent version of each file
        if base not in latest_files or os.path.getctime(f) > os.path.getctime(latest_files[base]):
            latest_files[base] = f
    return latest_files

def clean_dataframe(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Apply dynamic cleaning rules depending on file content.
    """
    try:
        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Dynamically detect and convert date/time columns
        for col in df.columns:
            if any(keyword in col for keyword in ["date", "time", "timestamp"]):
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Attempt numeric conversion for object columns
        for col in df.select_dtypes(include=["object"]).columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="ignore")
            except Exception:
                pass

        # Fill missing values dynamically
        df = df.fillna({
            col: 0 if df[col].dtype.kind in "biufc" else "unknown"
            for col in df.columns
        })

        logger.info(f"Cleaned dataframe for {filename} with {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Error cleaning {filename}: {e}")
        return df

def save_dataframe(df: pd.DataFrame, filename: str):
    """Save cleaned dataframe to processed folder."""
    try:
        out_path = os.path.join(PROCESSED_DIR, filename)
        df.to_csv(out_path, index=False)
        logger.info(f"Saved cleaned data → {out_path}")
    except Exception as e:
        logger.error(f"Error saving {filename}: {e}")



def main():
    logger.info("Starting transformation pipeline...")
    errors = 0

    try:
        latest_files = get_latest_files()

        for filename, path in latest_files.items():
            logger.info(f"Processing {filename} from {path}")
            df = pd.read_csv(path)
            df_clean = clean_dataframe(df, filename)

            if df_clean is None:   # cleaning failed
                errors += 1
                continue

            save_dataframe(df_clean, filename)

        if errors > 0:
            logger.error(f"Transformation completed with {errors} errors.")
        else:
            logger.info("Transformation completed successfully.")

    except Exception as e:
        logger.error(f"Transformation pipeline failed: {e}")



if __name__ == "__main__":
    main()