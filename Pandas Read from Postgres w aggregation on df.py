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

# SQL query to fetch raw data
query = "SELECT date, category, sales FROM public.sales_data"

start_time = time.time()

# Directly load data into a Pandas DataFrame using read_sql
df = pd.read_sql(query, engine)

#print(df.describe())

# Convert the 'date' column to datetime and extract the year
df['year'] = pd.to_datetime(df['date']).dt.year

#print(df.describe)

# Group by 'year' and 'category', and sum 'sales'
aggregated_df = df.groupby(['year', 'category']).agg(total_sales=pd.NamedAgg(column='sales', aggfunc='sum'))


end_time = time.time()

# Output DataFrame information and duration

duration = end_time - start_time
print(f"The data loading and aggregation took {duration} seconds with Pandas.")

#print(aggregated_df.head())
