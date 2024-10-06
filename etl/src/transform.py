def transform_data(df):
    # Example transformation: calculate total sales value
    df['total_value'] = df['quantity'] * df['product_price']
    return df
