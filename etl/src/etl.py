import logging
import yaml
from extract import extract_data
# from transform import transform_data
from load import load_data


# Setup logging configuration
logging.basicConfig(
    filename='./etl/logs/etl_process.log',  # Log to file
    level=logging.DEBUG,               # Log all levels from DEBUG and above
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def load_config(config_path='config.yml'):
    """Load the configuration file."""
    logging.debug("Loading configuration file.")
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logging.info("Configuration loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load configuration file: {e}")
        raise
    return config


# Main ETL process
def main():
    logging.info("Starting ETL process.")
    config = load_config("./etl/config/config.yml")
    try:
        # Extract
        df = extract_data(db_config=config['source_db'])
        logging.info("Data extracted successfully!")

        # Transform optional
        # df = transform_data(df)
        # logging.info("Data transformed successfully!")

        # Load
        load_data(df, db_config=config['target_db'],
                  target_table='monthly_sales')
        logging.info("Data loaded successfully into the target database.")
    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}")
    finally:
        logging.info("ETL process completed.")


if __name__ == "__main__":
    main()
