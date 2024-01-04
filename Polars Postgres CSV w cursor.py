import polars as pl
import sqlalchemy as al
import psycopg2
import configparser

#####
# Initialize the config parser and read the config file
config = configparser.ConfigParser()
config.read('C:\\Users\\Mkuli\\OneDrive\\Documents\\MyCode\\Polars\\config.ini')
# Database credentials
# Retrieve the database credentials from the config file
user = config['DATABASE']['User']
password = config['DATABASE']['Password']
host = config['DATABASE']['Host']
port = config['DATABASE']['Port']
db = config['DATABASE']['DatabaseName']
# print(config.ini)
conn = psycopg2.connect(
    dbname=db,
    user=user,
    password=password,
    host=host,
    port=port
)

cursor = conn.cursor()
query = "SELECT * FROM public.sales_data"
cursor.execute(query)

rows = cursor.fetchall()
# Get column names from cursor.description
column_names = [desc[0] for desc in cursor.description]

# Transpose rows to a dict with column data
data = {column: [row[i] for row in rows] for i, column in enumerate(column_names)}

df = pl.DataFrame(data)
file_path=r'C:\Users\Mkuli\OneDrive\Documents\test.csv'
df.write_csv(file_path)

cursor.close()
conn.close()
