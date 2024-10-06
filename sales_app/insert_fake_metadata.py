import psycopg2
from faker import Faker
import random
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Fetch database credentials from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Initialize Faker object
fake = Faker()

# Connect to your PostgreSQL database using the credentials from .env
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cur = conn.cursor()


# Function to insert user data
def insert_user_data(num_users):
    for _ in range(num_users):
        user_id = fake.random_int(min=1000, max=9999)
        name = fake.name()
        age = random.randint(18, 70)
        gender = random.choice(['M', 'F'])
        city = fake.city()
        cur.execute("""
            INSERT INTO user_metadata (user_id, name, age, gender, city)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, name, age, gender, city))


# Function to insert product data
def insert_product_data(num_products):
    for _ in range(num_products):
        product_id = fake.random_int(min=10000, max=99999)
        product_name = fake.word().capitalize()
        category = random.choice(['Electronics', 'Furniture',
                                  'Clothing', 'Toys'])
        price = round(random.uniform(10.0, 5000.0), 2)     
        cur.execute("""
            INSERT INTO product_metadata
                    (product_id, product_name, category, price)
            VALUES (%s, %s, %s, %s)
        """, (product_id, product_name, category, price))


insert_user_data(10)
insert_product_data(10)
conn.commit()
cur.close()
conn.close()

print("Data inserted successfully!")
