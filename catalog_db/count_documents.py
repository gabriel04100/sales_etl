import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URI from environment variable
MONGODB_URI = os.getenv('MONGODB_URI')


def count_documents(db_name, collection_name, filter={}):
    """Count documents in a specified collection."""
    try:
        # Connect to MongoDB
        with MongoClient(MONGODB_URI) as client:
            db = client[db_name]
            collection = db[collection_name]

            # Count documents
            document_count = collection.count_documents(filter)
            print("Total documents in '{}.{}': {}".format(
                db_name, collection_name, document_count
            ))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        client.close()  # Ensure the client connection is closed


if __name__ == "__main__":
    # filter_laptop = {"type": "laptop"}
    filter_phones = {"type": "smart phone", "screen size": 6}
    database_name = 'catalog'
    collection_name = 'electronics'

    count_documents(database_name, collection_name, filter=filter_phones)
