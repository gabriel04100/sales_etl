import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URI from environment variable
MONGODB_URI = os.getenv('MONGODB_URI')


def export_to_csv(db_name, collection_name, csv_file_path, selected_fields):
    """Export specified fields from a MongoDB collection to a CSV file."""
    try:
        with MongoClient(MONGODB_URI) as client:
            db = client[db_name]
            collection = db[collection_name]

            # Fetch documents from MongoDB
            cursor = collection.find({},
                                     {field: 1 for field in selected_fields})
            df = pd.DataFrame(list(cursor))

            # Convert types as needed
            # Example: Convert 'screen size' to float if it's not already
            if 'screen size' in df.columns:
                df['screen size'] = df['screen size'].astype(float)

            # Export to CSV
            df.to_csv(csv_file_path, index=False)
            print(f"Exported data to {csv_file_path}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Specify the database and collection names
    database_name = 'catalog'
    collection_name = 'electronics'
    csv_file_path = './data/electronics.csv'

    # Specify the fields to export
    selected_fields = ["_id", 'type', 'model']

    export_to_csv(database_name, collection_name,
                  csv_file_path, selected_fields)
