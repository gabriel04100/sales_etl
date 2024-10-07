from connector import create_connection
from psycopg2 import sql
import pandas as pd


def load_data(df, db_config, target_table):
    conn = create_connection(db_config)

    try:
        # Create a cursor
        cur = conn.cursor()

        # Create the main table if it does not exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            month DATE NOT NULL,  -- Use DATE type for better date handling
            product_category VARCHAR NULL,
            total_revenue FLOAT4 NULL,
            total_quantity INT4 NULL,
            revenue_performance VARCHAR NULL
        ) PARTITION BY RANGE (month);
        """
        cur.execute(create_table_query)
        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            month_partition = row['month']
            start_date = f"{month_partition}-01"
            end_date = pd.to_datetime(start_date) + pd.DateOffset(months=1)

            # Create the partition name
            partition_name = "{}_{}".format(
                target_table, month_partition.replace('-', '_')
            )

            # Create the partition for this month if it does not exist
            create_partition_query = f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF {target_table} FOR VALUES FROM
            ('{start_date}') TO ('{end_date}');
            """
            cur.execute(create_partition_query)

            # Insert row into the target table
            insert_query = sql.SQL("""
                INSERT INTO {table} (month, product_category, total_revenue,
                                     total_quantity,
                                     revenue_performance)
                VALUES (%s, %s, %s, %s, %s)
            """).format(table=sql.Identifier(target_table))
            cur.execute(insert_query, (start_date, row['product_category'],
                                       row['total_revenue'],
                                       row['total_quantity'],
                                       row['revenue_performance']))

        # Commit the transaction
        conn.commit()
        print("Data loaded successfully!")

    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        conn.rollback()

    finally:
        # Close cursor and connection
        cur.close()
        conn.close()
