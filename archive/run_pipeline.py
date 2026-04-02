from scripts.fetch_data import main as fetch_main
from scripts.transform_data import main as transform_main
from scripts.load_to_db import main as load_main
from scripts.api_ingestion import main as users_main
from scripts.logger import setup_logger
from scripts.ml_model import main as ml_main

# Initialize logger
logger = setup_logger()


def run_pipeline():
    logger.info("Starting full data pipeline...")

    try:
        logger.info("Step 1: Fetching data...")
        fetch_main()

        logger.info("Step 2: Fetching users data...")
        users_main()

        logger.info("Step 2: Transforming data...")
        transform_main()

        logger.info("Step 3: Loading data to database...")
        load_main()

        logger.info("Full pipeline executed successfully")

        logger.info("Step 4: Running ML model...")
        ml_main()

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    run_pipeline()