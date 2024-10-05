import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URI from environment variable
MONGODB_URI = os.getenv('MONGODB_URI')


def average_screen_size(db_name, collection_name):
    """Count documents in a specified collection."""
    try:
        # Connect to MongoDB
        with MongoClient(MONGODB_URI) as client:
            db = client[db_name]
            collection = db[collection_name]

            pipeline = [{"$match": {"type": "smart phone"}},
                        {"$group":
                        {"_id": None,
                         "averageScreenSize": {"$avg": "$screen size"}}}]

            result = list(collection.aggregate(pipeline))

            if result:
                average = result[0]['averageScreenSize']
                print(f"average screen size : {average}")
            else:
                print("No smartphone found")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        client.close()  # Ensure the client connection is closed


if __name__ == "__main__":
    database_name = 'catalog'
    collection_name = 'electronics'

    average_screen_size(database_name, collection_name)
