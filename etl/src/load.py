from connector import create_connection


# Load data into target database
def load_data(df, db_config, target_table):

    conn = create_connection(db_config)

    # Load the DataFrame into the target PostgreSQL database
    df.to_sql(target_table, conn, if_exists='replace', index=False)

    conn.close()
    print("Data loaded successfully!")
