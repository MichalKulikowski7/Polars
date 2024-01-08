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

# SQL query to select and aggregate data from the database
query = """
SELECT EXTRACT(YEAR FROM date) AS year, 
       category as cat, 
       sum(sales)
FROM public.sales_data
GROUP BY year, cat
"""

# Start the timer
start_time = time.time()

# Use ConnectorX to load data into a Polars DataFrame
uri = f'postgresql://{user}:{password}@{host}:{port}/{db}'
df = pl.read_database_uri(query=query, uri=uri, engine="adbc")

# Calculate the data loading duration
end_time_read = time.time()
duration_read = end_time_read - start_time

print(df)
print(f"The query and data loading took {duration_read} seconds with ConnectorX (ADBC)")
