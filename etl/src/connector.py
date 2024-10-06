import psycopg2


# Function to establish connection to PostgreSQL
def create_connection(db_config):
    conn = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )
    return conn
