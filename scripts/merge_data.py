# scripts/merge_data.py
import os
import pandas as pd
from sqlalchemy import create_engine

from config_loader import load_config
from logger import setup_logger

# Load config + logger
config = load_config()
logger = setup_logger()

MERGED_DIR = config["paths"]["merged_data"]
os.makedirs(MERGED_DIR, exist_ok=True)

# Postgres engine
engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

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
    fact_table = config["fact_table"]
    relationships = config.get("relationships", {})

    # Start with fact table columns
    select_parts = ["f.*"]
    join_parts = []

    for dim_name, rel in relationships.items():
        # Get dimension columns
        dim_cols = pd.read_sql_query(
            f"SELECT column_name FROM information_schema.columns WHERE table_name='{dim_name}'",
            engine
        )["column_name"].tolist()

        # Prefix dimension columns to avoid duplicates
        dim_select = ", ".join([f"d_{dim_name}.{c} AS {dim_name}_{c}" for c in dim_cols])
        select_parts.append(dim_select)

        # Add join clause
        join_parts.append(
            f"LEFT JOIN {dim_name} d_{dim_name} "
            f"ON f.{rel['fact_key']} = d_{dim_name}.{rel['dim_key']}"
        )

    # Build final query: one FROM, multiple LEFT JOINs
    query = f"SELECT {', '.join(select_parts)} FROM {fact_table} f " + " ".join(join_parts)

    merged_df = pd.read_sql_query(query, engine)
    logger.info(f"Merged fact table → {len(merged_df):,} rows")
    return merged_df

rename_map = {
    "olist_customers_dataset_clean_customer_unique_id": "cust_unique_id",
    "olist_customers_dataset_clean_customer_zip_code_prefix": "cust_zip_code_prefix",
    "olist_customers_dataset_clean_customer_city": "cust_city",
    "olist_customers_dataset_clean_customer_state": "cust_state",
    "olist_order_items_dataset_clean_order_item_id": "item_id",
    "olist_order_items_dataset_clean_product_id": "product_id",
    "olist_order_items_dataset_clean_seller_id": "seller_id",
    "olist_order_items_dataset_clean_shipping_limit_date": "shipping_limit_date",
    "olist_order_items_dataset_clean_price": "price",
    "olist_order_items_dataset_clean_freight_value": "freight_value",
    "olist_order_payments_dataset_clean_payment_sequential": "pay_sequential",
    "olist_order_payments_dataset_clean_payment_type": "pay_type",
    "olist_order_payments_dataset_clean_payment_installments": "pay_installments",
    "olist_order_payments_dataset_clean_payment_value": "pay_value",
    "olist_order_reviews_dataset_clean_review_id": "review_id",
    "olist_order_reviews_dataset_clean_review_score": "review_score",
    "olist_order_reviews_dataset_clean_review_comment_title": "review_comment_title",
    "olist_order_reviews_dataset_clean_review_comment_message": "review_comment",
    "olist_order_reviews_dataset_clean_review_creation_date": "review_creation_date",
    "olist_order_reviews_dataset_clean_review_answer_timestamp": "review_answer_timestamp"
}



def save_fact_table(df):
    out_path_csv = os.path.join(MERGED_DIR, "fact_orders_star.csv")
    out_path_parquet = os.path.join(MERGED_DIR, "fact_orders_star.parquet")

    df.to_csv(out_path_csv, index=False)
    df.to_parquet(out_path_parquet, index=False)

    logger.info(f"Saved merged fact table → {out_path_csv} and {out_path_parquet}")


def main():
    logger.info("Starting merge pipeline...")
    fact_df = merge_tables()
    if fact_df is not None:
        # Apply renaming here
        fact_df = fact_df.rename(columns=rename_map)

        save_fact_table(fact_df)
        logger.info("Merge pipeline completed successfully.")
    else:
        logger.error("Merge pipeline failed.")

if __name__ == "__main__":
    main()
