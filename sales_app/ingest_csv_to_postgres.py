import os
import glob
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(filename='./sales_app/logs/ingestion.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Retrieve PostgreSQL connection details from environment variables
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def find_latest_csv(directory='./sales_app/data/'):
    """Find the latest CSV file in the given directory."""
    csv_files = glob.glob(f"{directory}/*.csv")
    if not csv_files:
        logging.error("No CSV files found.")
        raise FileNotFoundError("No CSV files found.")
    # Sort files by modified time and return the latest one
    latest_file = max(csv_files, key=os.path.getmtime)
    logging.info(f"Found latest CSV file: {latest_file}")
    return latest_file


def ingest_csv_to_postgres(csv_file):
    """Ingest the CSV file into the PostgreSQL database."""
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    # Create a cursor object
    cur = conn.cursor()

    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)

        # Iterate through the DataFrame rows and insert them into the database
        for index, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO sales
                (timestamp, product_id, customer_id, price, quantity)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (row['timestamp'], row['product_id'],
                 row['customer_id'], row['price'], row['quantity'])
            )

        # Commit the transaction
        conn.commit()

        logging.info(f"Successfully ingested {len(df)} rows from {csv_file}")

    except Exception as e:
        # Rollback in case of an error
        conn.rollback()
        logging.error(f"Failed to ingest CSV into PostgreSQL: {e}")
        raise

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        # Find the latest CSV file
        latest_csv = find_latest_csv()

        # Ingest the latest CSV into the PostgreSQL database
        ingest_csv_to_postgres(latest_csv)

    except Exception as e:
        logging.error(f"An error occurred during ingestion: {e}")
