import numpy as np
import csv
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(filename='./sales_app/logs/sales_simulation.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(message)s')


# Parameters for sales simulation
def simulate_sales_per_day(lam=2000):
    records = []

    # Simulating sales from 9 AM to 9 PM
    start_time = datetime.now().replace(hour=9, minute=0, second=0,
                                        microsecond=0)
    # Number of sales to simulate for the day
    sales_count = np.random.poisson(lam=lam)

    for _ in range(sales_count):
        # Randomly generate a timestamp for the sale within the 12-hour window
        time_offset = np.random.uniform(0, 12 * 60 * 60)  # Seconds in 12 hours
        sale_time = start_time + timedelta(seconds=time_offset)

        # Generate the random variables for the sale
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


def append_records_to_csv(records,
                          filename='./sales_app/data/sales_data.csv'):
    # Specify the CSV file headers
    headers = ['timestamp', 'product_id', 'customer_id', 'price', 'quantity']

    # Open the CSV file in append mode, and write the sales records
    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        # Write headers only if the file is empty (i.e., on the first run)
        if file.tell() == 0:
            writer.writeheader()

        # Write the records
        writer.writerows(records)


if __name__ == "__main__":
    try:
        # Simulate sales
        records = simulate_sales_per_day(lam=2000)

        # Append records to CSV
        append_records_to_csv(records)

        # Log success message
        logging.info(f"Successfully simulated {len(records)}")

    except Exception as e:
        # Log any errors
        logging.error(f"An error occurred: {e}")
