import connectorx as cx
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
conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"

# SQL query
query = "SELECT * FROM public.sales_data"

# Start the timer
start_time = time.time()

# Use ConnectorX to load data into a Polars DataFrame
df = cx.read_sql(conn_str, query, return_type="polars")
end_time_read = time.time()
# Export to CSV
file_path = r'C:\Users\Mkuli\OneDrive\Documents\Files Repo\test_connectorx.csv'

df.write_csv(file_path)
end_time = time.time()
# Calculate the load duration
duration_read = end_time_read - start_time
# Calculate the total duration
duration_total = end_time - start_time
print(f"The query and data loading took {duration_read} seconds with ConnectorX (not ADBC) and total duration was {duration_total}.")
