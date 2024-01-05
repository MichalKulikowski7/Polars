import pandas as pd
import sqlalchemy as al
import configparser
import time

# Init the config parser and read the config file
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
query = "SELECT * FROM public.sales_data limit 50000000"


# Start the timer
start_time = time.time()

# Directly load data into a Pandas DataFrame using read_sql
df = pd.read_sql(query, engine)

# End the timer
end_time = time.time()
print(df.dtypes)
print(df.describe)
# Calculate the duration
duration = end_time - start_time
print(f"The query and data loading took {duration} seconds with Pandas.")
