import requests
import psycopg2
import pandas as pd
import time
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType

# Create a Spark session
spark = SparkSession.builder.appName("CoinGeckoStreamingApp").getOrCreate()

# Create a StreamingContext with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 60)
# Define the schema for the streaming data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("current_price", StringType(), True),
    StructField("last_updated", StringType(), True)
])

# Define the CoinGecko API endpoint
api_url = "https://api.coingecko.com/api/v3/coins/markets"

# Function to fetch data from the CoinGecko API
def fetch_coingecko_data():
    response = requests.get(api_url, params={"vs_currency": "usd"})
    if response.status_code == 200:
        return response.json()
    else:
        return []

# Function to write DataFrame to PostgreSQL
def write_to_postgres(df):
    # Convert DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="coin",
        user="data_eng",
        password="data_eng"
    )

    # Create a cursor
    cursor = conn.cursor()

    # Check if the table exists and create it if not
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'coingecko_data')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        cursor.execute("""
            CREATE TABLE coingecko_data (
                id VARCHAR(255),
                name VARCHAR(255),
                symbol VARCHAR(255),
                current_price DECIMAL(18, 8),
                last_updated TIMESTAMP
            )
        """)
        conn.commit()

    # Convert the data types of the DataFrame columns
    df = df.withColumn("current_price", df["current_price"].cast(DecimalType(18, 8)))
    df = df.withColumn('last_updated', unix_timestamp(df['last_updated'], 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'').cast(LongType()))
    # Write data to PostgreSQL table
    for _, row in pandas_df.iterrows():
        cursor.execute(
            "INSERT INTO coingecko_data (id, name, symbol, current_price, last_updated) VALUES (%s, %s, %s, %s, %s)",
            (row["id"], row["name"], row["symbol"], row["current_price"], row["last_updated"])
        )

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


while True:
    # Fetch data from the CoinGecko API
    data = fetch_coingecko_data()

    # Create a DataFrame from the fetched data
    df = spark.createDataFrame(data, schema)

    # Perform any required transformations or computations on the DataFrame
    # For example, you can filter for specific currencies or calculate aggregate statistics

    # Write the DataFrame to PostgreSQL
    write_to_postgres(df)

    # Sleep for a desired interval before fetching data again
    time.sleep(60)  # Fetch data every 60 seconds


# Start the streaming context
ssc.start()
ssc.awaitTermination()