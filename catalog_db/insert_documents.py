import json
import logging
import os
from pymongo import MongoClient, errors
from dotenv import load_dotenv

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from .env file located in the same directory
load_dotenv(os.path.join(script_dir, '.env'))

# Get MongoDB URI from environment variable
MONGODB_URI = os.getenv('MONGODB_URI')

# Configure logging
log_file_path = os.path.join(script_dir, 'data', 'insert_log.log')
logging.basicConfig(
    filename=log_file_path,  # Path to log file
    level=logging.INFO,  # Logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'
)

def load_json_file(json_file_path):
    """Load and return the contents of a JSON file with multiple JSON objects."""
    data = []
    try:
        with open(json_file_path, 'r') as file:
            for line in file:
                # Convert each line (JSON object) to a dictionary and append to the list
                data.append(json.loads(line.strip()))
        logging.info(f"Successfully loaded JSON file: {json_file_path}")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {json_file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from file: {json_file_path}, Error: {e}")
        raise

def insert_into_mongodb(data, db_name, collection_name):
    """Insert data into MongoDB collection."""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URI)
        db = client[db_name]
        collection = db[collection_name]

        # Insert JSON data into the collection
        if data:
            result = collection.insert_many(data)
            logging.info(f"Inserted {len(result.inserted_ids)} documents into '{db_name}.{collection_name}'")
        else:
            logging.warning(f"No data to insert into '{db_name}.{collection_name}'")

    except errors.ConnectionError as ce:
        logging.error(f"Connection error: {ce}")
        raise
    except errors.PyMongoError as e:
        logging.error(f"MongoDB insertion error: {e}")
        raise
    finally:
        client.close()
        logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    # Path to the .json file located in the data directory
    json_file_path = os.path.join(script_dir, 'data', 'catalog.json')
    
    # Load JSON data
    try:
        data = load_json_file(json_file_path)
    except Exception as e:
        logging.critical(f"Failed to load or parse JSON file: {e}")
        exit(1)  # Exit if there's an issue with the file

    # Insert JSON data into MongoDB
    try:
        insert_into_mongodb(data, db_name='catalog', collection_name='electronics')
    except Exception as e:
        logging.critical(f"Failed to insert data into MongoDB: {e}")
        exit(1)  # Exit if the insertion fails

    print("Data insertion complete. Check logs for details.")




