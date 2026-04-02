import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

from scripts.config_loader import load_config
from scripts.logger import setup_logger

# Load config + logger
config = load_config()
logger = setup_logger()

DATA_PATH = config["paths"]["processed_data"]


def load_data():
    try:
        df = pd.read_csv(DATA_PATH)
        logger.info("Data loaded for ML")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None


def prepare_data(df):
    try:
        # 🎯 Create target column
        df["high_value"] = (df["price"] > df["price"].mean()).astype(int)

        # Features
        X = df[["price", "rating", "stock"]]

        # Target
        y = df["high_value"]

        logger.info("Data prepared for training")
        return X, y

    except Exception as e:
        logger.error(f"Error preparing data: {e}")
        return None, None


def train_model(X, y):
    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = LogisticRegression()
        model.fit(X_train, y_train)

        predictions = model.predict(X_test)

        accuracy = accuracy_score(y_test, predictions)

        logger.info(f"Model trained successfully. Accuracy: {accuracy:.2f}")

        return model

    except Exception as e:
        logger.error(f"Error training model: {e}")
        return None


def main():
    logger.info("Starting ML pipeline...")

    df = load_data()

    if df is None:
        logger.error("ML pipeline failed at data loading")
        return

    X, y = prepare_data(df)

    if X is None:
        logger.error("ML pipeline failed at data preparation")
        return

    train_model(X, y)

    logger.info("ML pipeline completed successfully")


if __name__ == "__main__":
    main()