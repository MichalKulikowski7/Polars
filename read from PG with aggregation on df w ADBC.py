import configparser
import polars as pl
import time

# Initialize the config parser and read the config file
config = configparser.ConfigParser()
config.read('C:\\Users\\Mkuli\\OneDrive\\Documents\\MyCode\\Polars\\config.ini')

# Database credentials and connection string
user = config['DATABASE']['User']
password = config['DATABASE']['Password']
host = config['DATABASE']['Host']
port = config['DATABASE']['Port']
db = config['DATABASE']['DatabaseName']

# SQL query to select data from the database
query = "SELECT date, category, sales FROM public.sales_data"

# Start the timer
start_time = time.time()

# Use ADBC to load data into a Polars DataFrame
uri = f'postgresql://{user}:{password}@{host}:{port}/{db}'
df = pl.read_database_uri(query=query, uri=uri, engine="adbc")

print(df.describe())
# Extract the year from the 'date' column
df = df.with_columns([
    pl.col("date").dt.year().alias("year")
])

# Group by 'year' and 'category', and sum 'sales'
aggregated_df = df.group_by(["year", "category"]).agg([
    pl.col("sales").sum().alias("total_sales")
])

# Calculate the data loading and processing duration
end_time_read = time.time()
duration_read = end_time_read - start_time

print(f"The data loading and aggregation took {duration_read} seconds with ADBC")
print(aggregated_df.head())
