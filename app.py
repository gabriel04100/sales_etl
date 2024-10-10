import streamlit as st
from dotenv import load_dotenv
from pygwalker.api.streamlit import StreamlitRenderer

# Load environment variables if necessary (e.g., for database credentials)
load_dotenv()

# Streamlit page configuration
st.set_page_config(page_title="Monthly Sales", page_icon=None, layout="wide")

# SQL query to fetch data
query = "SELECT * from monthly_sales;"

# Fetch data from PostgreSQL using Streamlit's connection
conn = st.connection("postgresql", type="sql")
sales_df = conn.query(query)

# Display the dataframe using Streamlit's dataframe function
st.dataframe(sales_df, use_container_width=True, hide_index=True)

pyg_app = StreamlitRenderer(sales_df)
pyg_app.explorer()
