# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respxzective databases
import sqlite3
import duckdb
import pandas as pd
# Fetch data from the SQLite Customer table
conn_sq = sqlite3.connect('tpch.db')
df = pd.read_sql_query('SELECT * FROM Customer', conn_sq)
# Insert data into the DuckDB Customer table
conn_ddb = duckdb.connect('duckdb.db')
insert_sql = """
                INSERT INTO Customer SELECT * FROM df
            """
# conn_ddb.execute(insert_sql)

print('Customer rows in duckdb:')
print(conn_ddb.sql('SELECT * FROM Customer'))

conn_sq.close()
conn_ddb.close()
# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import gzip
from io import BytesIO, TextIOWrapper


# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# Download the CSV file from S3
# Decompress the gzip data
# Read the CSV file using csv.reader
# Connect to the DuckDB database (assume WeatherData table exists)
conn_ddb = duckdb.connect('duckdb.db')

# Download the gzipped file from S3
response = s3.get_object(Bucket=bucket_name, Key=file_key)
with gzip.open(BytesIO(response['Body'].read()), 'rb') as gz_file:
    with TextIOWrapper(gz_file, encoding='utf-8') as csv_file:
        df = pd.read_csv(csv_file)

# Display the DataFrame
print('Downloaded df')
print(df.head())
# Insert data into the DuckDB WeatherData table
# conn_ddb.execute('INSERT INTO WeatherData SELECT * FROM df')
print('Weather data contents')
print(conn_ddb.sql('SELECT * FROM WeatherData'))
conn_ddb.close()
# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"
import requests

# Fetch data from the CoinCap API
# Connect to the DuckDB database
response = requests.get(url)
x = response.json()
df = pd.DataFrame(x.get('data'))
df = df.astype({'rank': int, 'percentTotalVolume': float, 'volumeUsd': float, 'socket': bool, 'updated': float})
# Insert data into the DuckDB Exchanges table
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py
print('API df data types')
print(df.info())
conn_ddb = duckdb.connect('duckdb.db')

column_info = conn_ddb.execute("PRAGMA table_info('Exchanges')").fetchall()

# Print the column names and types
for col in column_info:
    print(f"Column Name: {col[1]}, Data Type: {col[2]}")

# conn_ddb.execute('INSERT INTO Exchanges SELECT * FROM df')
print('Exchanges data contents')
print(conn_ddb.sql('SELECT * FROM Exchanges'))
conn_ddb.close()

# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python

# import csv
# with open('Giants.csv', mode ='r') as file:    
#        csvFile = csv.DictReader(file)
# Then import the csvFile as dict into a desired database
# Or
# Read using pandas library into a Dataframe and later import


# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape

import requests
import bs4

url = 'https://example.com'

response = requests.get(url)
soup = bs4.BeautifulSoup(response.content, 'html.parser')
links = soup.find_all("a") 
for link in links:
  print("Link:", link.get("href"), "Text:", link.string)
