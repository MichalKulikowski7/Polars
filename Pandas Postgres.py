import pandas as pd
import sqlalchemy as al
import configparser
import time

# Initialize the config parser and read the config file
config = configparser.ConfigParser()
config.read('C:\\Users\\Mkuli\\OneDrive\\Documents\\MyCode\\Polars\\config.ini')

# Database credentials
user = config['DATABASE']['User']
password = config['DATABASE']['Password']
host = config['DATABASE']['Host']
port = config['DATABASE']['Port']
db = config['DATABASE']['DatabaseName']

# Create SQLAlchemy engine
engine = al.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

# SQL query
query = "SELECT * FROM public.sales_data"

# Start the timer
start_time = time.time()

# Directly load data into a Pandas DataFrame using read_sql
df = pd.read_sql(query, engine)

# Export to CSV
file_path = r'C:\Users\Mkuli\OneDrive\Documents\test_pandas.csv'
df.to_csv(file_path)

# End the timer
end_time = time.time()

# Calculate the duration
duration = end_time - start_time
print(f"The query and data loading took {duration} seconds with Pandas.")
