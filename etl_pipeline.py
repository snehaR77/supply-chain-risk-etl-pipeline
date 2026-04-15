import pandas as pd
import sqlite3
import logging
from datetime import datetime
from config import DB_NAME, DATA_PATH

logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO)

def extract():
    logging.info("Extracting data...")
    return pd.read_csv(DATA_PATH)

def transform(df):
    logging.info("Transforming data...")
    df = df.dropna()
    df['delay_days'] = (pd.to_datetime(df['actual_date']) - pd.to_datetime(df['expected_date'])).dt.days
    df['price_volatility'] = df['price'].pct_change().fillna(0)
    df['risk_score'] = (
        df['delay_days'] * 0.5 +
        df['price_volatility'] * 0.3 +
        df['demand'] * 0.2
    )
    return df

def load(df):
    logging.info("Loading data into SQL...")
    conn = sqlite3.connect(DB_NAME)
    df.to_sql("risk_metrics", conn, if_exists="append", index=False)
    conn.close()

def run_pipeline():
    try:
        logging.info(f"Pipeline started at {datetime.now()}")
        df = extract()
        df = transform(df)
        load(df)
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()
