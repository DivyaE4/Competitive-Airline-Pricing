import os
import time
import sys

# Add the parent directory to sys.path to allow importing spark_queries
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Import your existing query functions
import spark_queries

# Print debug information
print("Script started successfully!")

# Define schema for flight price data
FLIGHT_PRICE_SCHEMA = StructType([
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Configuration constants
WINDOW_DURATION_SECONDS = 60
SLIDE_DURATION_SECONDS = 30
KAFKA_TOPICS = ["sfo_sea_prices", "ord_mia_prices", "jfk_lax_prices"]

# PostgreSQL connection properties
DB_PROPERTIES = {
    "user": "airlineuser",
    "password": "airlinepass",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    """Create a Spark session with the required configuration"""
    print("Creating Spark session...")
    return (SparkSession.builder
            .appName("AirlinePriceStreaming")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
            .getOrCreate())

def process_streaming_data(spark):
    """Process streaming data from Kafka"""
    print("Setting up Kafka streaming source...")
    
    # First, check if Kafka topic exists - if not, create it
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        print(f"Existing Kafka topics: {existing_topics}")
        
        # Create topics if they don't exist
        new_topics = []
        for topic in KAFKA_TOPICS:
            if topic not in existing_topics:
                new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        
        if new_topics:
            print(f"Creating missing topics: {[t.name for t in new_topics]}")
            admin_client.create_topics(new_topics)
            print("Topics created successfully")
        
        admin_client.close()
    except Exception as e:
        print(f"Warning: Failed to check or create Kafka topics: {str(e)}")
        print("Continuing with the assumption that topics exist...")
    
    # Read from Kafka
    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", ",".join(KAFKA_TOPICS))
          .option("startingOffsets", "latest")
          .option("failOnDataLoss", "false")  # Add this to handle missing topics more gracefully
          .load())
    
    print("Parsing JSON data...")
    # Parse JSON data
    parsed_df = df.select(
        F.from_json(F.col("value").cast("string"), FLIGHT_PRICE_SCHEMA).alias("data"),
        F.col("topic"),
        F.col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "topic", "kafka_timestamp")
    
    # Register temporary view for SQL queries
    parsed_df.createOrReplaceTempView("flight_prices")
    
    # Define window duration
    window_duration = f"{WINDOW_DURATION_SECONDS} seconds"
    slide_duration = f"{SLIDE_DURATION_SECONDS} seconds"
    
    print("Creating analytical queries...")
    # Create query results
    route_stats = spark_queries.get_min_max_avg_prices_by_route(spark, window_duration)
    best_airlines = spark_queries.get_best_airline_by_route(spark, window_duration)
    price_trends = spark_queries.get_price_trends_by_route(spark, window_duration)
    
    # Write results to PostgreSQL
    def write_to_postgres(df, output_mode, table_name):
        print(f"Setting up streaming write to PostgreSQL table: {table_name}")
        checkpoint_loc = f"/tmp/checkpoints/{table_name}"
        return (df.writeStream
                .foreachBatch(lambda batch_df, batch_id: save_batch_to_postgres(batch_df, batch_id, table_name))
                .outputMode(output_mode)
                .option("checkpointLocation", checkpoint_loc)
                .start())
    
    print("Starting streaming queries...")
    # Start streaming queries
    # Use "complete" mode for aggregated data
    route_stats_query = write_to_postgres(route_stats, "complete", "route_stats")
    best_airlines_query = write_to_postgres(best_airlines, "complete", "best_airlines")
    price_trends_query = write_to_postgres(price_trends, "complete", "price_trends")
    
    # Use "append" mode for raw data
    raw_data_query = write_to_postgres(parsed_df, "append", "raw_flight_prices")
    
    print("All streaming queries started. Waiting for termination...")
    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Stream exception: {str(e)}")
        # Don't immediately exit - try to process a bit longer
        time.sleep(30)
        spark.streams.awaitAnyTermination() 

def save_batch_to_postgres(batch_df, batch_id, table_name):
    """Save a batch of data to PostgreSQL"""
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping write to {table_name}")
        return
    
    url = "jdbc:postgresql://localhost:5432/airlinepricing"
    
    try:
        # Try to create table if it doesn't exist
        try:
            # First, for safety, derive the schema from the DataFrame
            create_table_sql = derive_create_table_sql(batch_df, table_name)
            
            # Execute create table if not exists
            create_db_connection(url, DB_PROPERTIES, create_table_sql)
        except Exception as table_error:
            print(f"Note: Table creation attempted but may already exist: {str(table_error)}")
        
        # Write to PostgreSQL
        batch_df.write \
            .jdbc(url=url, table=table_name, mode="append", properties=DB_PROPERTIES)
        
        print(f"Batch {batch_id} written to PostgreSQL table {table_name}")
    except Exception as e:
        print(f"Error writing batch {batch_id} to PostgreSQL table {table_name}: {str(e)}")
        import traceback
        traceback.print_exc()

def derive_create_table_sql(df, table_name):
    """Derive CREATE TABLE SQL from DataFrame schema"""
    # Map Spark types to PostgreSQL types
    type_mapping = {
        "StringType": "TEXT",
        "DoubleType": "DOUBLE PRECISION",
        "IntegerType": "INTEGER",
        "LongType": "BIGINT",
        "TimestampType": "TIMESTAMP",
        "DateType": "DATE",
        "BooleanType": "BOOLEAN"
    }
    
    columns = []
    for field in df.schema.fields:
        spark_type = str(field.dataType).split("(")[0]
        pg_type = type_mapping.get(spark_type, "TEXT")
        columns.append(f"\"{field.name}\" {pg_type}")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    return create_sql

def create_db_connection(url, properties, sql):
    """Create a JDBC connection and execute SQL"""
    import jaydebeapi
    
    conn = jaydebeapi.connect(
        properties["driver"],
        url,
        [properties["user"], properties["password"]],
        "/path/to/postgresql-jdbc.jar"  # Update this path to your JDBC driver
    )
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
    finally:
        conn.close()

if __name__ == "__main__":
    print("Starting Spark Streaming processor...")
    try:
        # Add a small delay to ensure Kafka is up
        print("Waiting for Kafka to be fully ready...")
        time.sleep(5)
        
        spark = create_spark_session()
        process_streaming_data(spark)
    except Exception as e:
        print(f"Error in Spark Streaming processor: {str(e)}")
        import traceback
        traceback.print_exc()