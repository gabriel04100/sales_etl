import numpy as np
import csv
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    filename='./sales_app/logs/sales_simulation.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)


# Parameters for sales simulation
def simulate_sales_per_day(lam=2000, simulation_date=None):
    records = []

    # If no date is specified, use the current date
    if simulation_date is None:
        simulation_date = datetime.now()

    # Start time for the simulation (9 AM)
    start_time = simulation_date.replace(hour=9, minute=0,
                                         second=0, microsecond=0)

    # Generate sales count using Poisson distribution
    sales_count = np.random.poisson(lam=lam)

    for _ in range(sales_count):
        # Randomly generate a timestamp for the sale within the 12-hour window
        time_offset = np.random.uniform(0, 12 * 60 * 60)
        sale_time = start_time + timedelta(seconds=time_offset)

        # Generate random variables for the sale
        product_id = np.random.randint(low=10000, high=99999)
        customer_id = np.random.randint(low=10000, high=99999)
        price = np.random.uniform(low=1, high=5000)
        quantity = np.random.randint(low=1, high=10)

        # Create the sale record
        record = {
            'timestamp': sale_time.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product_id,
            'customer_id': customer_id,
            'price': price,
            'quantity': quantity
        }

        # Add the record to the list
        records.append(record)

    return records


def append_records_to_csv(records, simulation_date=None):
    # Use the specified date for the filename; default to current date
    if simulation_date is None:
        current_date = datetime.now().strftime('%Y-%m-%d')
    else:
        current_date = simulation_date.strftime('%Y-%m-%d')

    filename = f'./sales_app/data/sales_data_{current_date}.csv'

    headers = ['timestamp', 'product_id', 'customer_id', 'price', 'quantity']

    # Open the CSV file in append mode and write the sales records
    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        # Write headers only if the file is empty (i.e., on the first run)
        if file.tell() == 0:
            writer.writeheader()

        writer.writerows(records)


def main(simulation_date=None):
    try:
        records = simulate_sales_per_day(lam=2000,
                                         simulation_date=simulation_date)
        append_records_to_csv(records, simulation_date=simulation_date)
        logging.info(f"Successfully simulated {len(records)} sales records.")

    except Exception as e:
        # Log any errors
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    # Define the start and end dates for the simulation
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 12, 31)

    # Loop over each day in the specified date range
    current_date = start_date
    while current_date <= end_date:
        try:
            # Run the simulation for the current date
            records = simulate_sales_per_day(lam=2000,
                                             simulation_date=current_date)
            append_records_to_csv(records, simulation_date=current_date)
            logging.info(f"Successfully simulated {len(records)}")

        except Exception as e:
            # Log any errors for the current date
            logging.error("An error occurred for {}: {}".format(
                current_date.strftime('%Y-%m-%d'), e
            ))

        # Move to the next day
        current_date += timedelta(days=1)
