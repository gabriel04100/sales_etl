import pandas as pd
from connector import create_connection


# Extract data from source database
def extract_data(db_config):
    conn = create_connection(db_config)
    query = """
    SELECT
        s.sales_id,
        s.sale_date,
        s.quantity,
        s.sales_value,
        u.user_id,
        u.name AS user_name,
        u.age,
        u.gender,
        u.city,
        p.product_id,
        p.product_name,
        p.category AS product_category,
        p.price AS product_price
    FROM
        sales s
    JOIN
        user_metadata u ON s.user_id = u.user_id
    JOIN
        product_metadata p ON s.product_id = p.product_id;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df
