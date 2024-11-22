# Databricks notebook source
# MAGIC %pip install openpyxl==3.0.10

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
import sqlite3

# 1. Extract Data - Function to read CSV files
def extract_data(region_a_file, region_b_file):
    # Reading the CSV files for both regions
    region_a_df = pd.read_excel(region_a_file)
    region_b_df = pd.read_excel(region_b_file)
    
    # Add a column 'region' to each dataframe
    region_a_df['region'] = 'A'
    region_b_df['region'] = 'B'
    
    # Combine both dataframes into one
    combined_df = pd.concat([region_a_df, region_b_df], ignore_index=True)
    
    return combined_df

# 2. Transform Data - Applying business rules
def transform_data(df):
    # Remove duplicates based on OrderId
    df = df.drop_duplicates(subset=['OrderId'])
    
    # Convert PromotionDiscount to numeric, forcing errors to NaN
    df['PromotionDiscount'] = pd.to_numeric(df['PromotionDiscount'], errors='coerce').fillna(0)
    
    # Calculate total_sales = QuantityOrdered * ItemPrice
    df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
    
    # Calculate net_sale = total_sales - PromotionDiscount
    df['net_sale'] = df['total_sales'] - df['PromotionDiscount']
    
    # Exclude orders where net_sale is <= 0
    df = df[df['net_sale'] > 0]
    
    return df

# 3. Load Data into SQLite Database
def load_data_to_db(df, db_name='sales_data.db'):
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create the sales_data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS sales_data (
                        OrderId INTEGER PRIMARY KEY,
                        OrderItemId INTEGER,
                        QuantityOrdered INTEGER,
                        ItemPrice REAL,
                        PromotionDiscount REAL,
                        total_sales REAL,
                        region TEXT,
                        net_sale REAL)''')
    
    # Insert the data into the sales_data table
    df.to_sql('sales_data', conn, if_exists='replace', index=False)
    
    # Commit and close the connection
    conn.commit()
    conn.close()

# 4. SQL Queries to Validate Data
def validate_data(db_name='sales_data.db'):
    # Connect to SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # a. Count the total number of records
    cursor.execute('SELECT COUNT(*) FROM sales_data')
    total_records = cursor.fetchone()[0]
    print(f"Total number of records: {total_records}")
    
    # b. Find the total sales amount by region
    cursor.execute('SELECT region, SUM(total_sales) FROM sales_data GROUP BY region')
    total_sales_by_region = cursor.fetchall()
    print(f"Total sales by region: {total_sales_by_region}")
    
    # c. Find the average sales amount per transaction
    cursor.execute('SELECT AVG(total_sales) FROM sales_data')
    avg_sales_per_transaction = cursor.fetchone()[0]
    print(f"Average sales per transaction: {avg_sales_per_transaction}")
    
    # d. Ensure there are no duplicate OrderId values
    cursor.execute('SELECT COUNT(DISTINCT OrderId), COUNT(OrderId) FROM sales_data')
    distinct_order_ids, total_order_ids = cursor.fetchone()
    if distinct_order_ids == total_order_ids:
        print("No duplicate OrderIds found.")
    else:
        print("There are duplicate OrderIds.")
    
    # Close the connection
    conn.close()

# Example of using the functions

# 1. Extract the data from CSV files
region_a_file = '/Workspace/Data/order_region_a.xlsx'  # Replace with actual file path
region_b_file = '/Workspace/Data/order_region_b.xlsx'  # Replace with actual file path
combined_df = extract_data(region_a_file, region_b_file)

# 2. Transform the data based on business rules
transformed_df = transform_data(combined_df)

# 3. Load the transformed data into SQLite database
load_data_to_db(transformed_df)

# 4. Validate the data by executing SQL queries
validate_data()
