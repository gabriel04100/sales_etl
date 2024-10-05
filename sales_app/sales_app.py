import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
load_dotenv()

# Database connection details from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Ensure logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging
logging.basicConfig(
    filename='logs/sales_data.log',  # Updated log file path
    level=logging.INFO,                # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

# Streamlit app title
st.title("Sales Data")


# Function to fetch sales data
def fetch_sales_data():
    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) as conn:
        # Query to fetch data from the sales table
        query = "SELECT * FROM sales ORDER BY timestamp DESC"
        return pd.read_sql(query, conn)


# Display the sales data
st.subheader("Sales Data Overview")
sales_data = fetch_sales_data()
st.dataframe(sales_data)

# Input fields for new sales
with st.form(key='insert_sales_form'):
    timestamp = st.date_input("Date of Sale")
    product_id = st.number_input("Product ID", min_value=1)
    customer_id = st.number_input("Customer ID", min_value=1)
    quantity = st.number_input("Quantity", min_value=1)
    price = st.number_input("Price per Unit", min_value=0.0, format="%.2f")
    submit_button = st.form_submit_button(label='Insert Sale')

    if submit_button:
        # Use the 'with' statement to handle the database connection
        with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            with conn.cursor() as cursor:
                # Insert new sales record
                insert_query = sql.SQL(
                    "INSERT INTO sales " +
                    "(timestamp, product_id, customer_id, quantity, price) " +
                    "VALUES (%s, %s, %s, %s, %s)"
                )
                try:
                    cursor.execute(insert_query,
                                   (timestamp, product_id, customer_id,
                                    quantity, price))
                    conn.commit()
                    logging.info(
                        "Inserted sale: Timestamp=%s, Product ID=%s, "
                        "Customer ID=%s, Quantity=%s, Price=%s",
                        timestamp,
                        product_id,
                        customer_id,
                        quantity,
                        price
                    )
                    st.success("Sale inserted successfully!")

                except Exception as e:
                    # Log any exceptions that occur
                    logging.error(f"Error inserting sale: {e}")
                    st.error("Error inserting sale")
