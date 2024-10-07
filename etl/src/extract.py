import pandas as pd
from connector import create_connection


def extract_data(db_config):
    conn = create_connection(db_config)
    query_monthly_sales_report = """
    SELECT
        to_char(s."timestamp", 'YYYY-MM') AS month,
        p.category AS product_category,
        ROUND(CAST(SUM(s.price * s.quantity) AS numeric), 2) AS total_revenue,
        CASE
            WHEN NTILE(4) OVER (PARTITION BY to_char(s."timestamp", 'YYYY-MM')
            ORDER BY SUM(s.price * s.quantity)) = 1 THEN 'Low'
            WHEN NTILE(4) OVER (PARTITION BY to_char(s."timestamp", 'YYYY-MM')
            ORDER BY SUM(s.price * s.quantity)) = 2 THEN 'Medium-Low'
            WHEN NTILE(4) OVER (PARTITION BY to_char(s."timestamp", 'YYYY-MM')
            ORDER BY SUM(s.price * s.quantity)) = 3 THEN 'Medium-High'
            ELSE 'High'
        END AS revenue_performance
    FROM
        sales s
    JOIN
        product_metadata p ON s.product_id = p.product_id
    GROUP BY
        month, product_category;
    """
    return pd.read_sql(query_monthly_sales_report, conn)
