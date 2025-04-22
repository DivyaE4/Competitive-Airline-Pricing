"""
Configuration for Spark Streaming windows
"""
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Window duration in seconds
WINDOW_DURATION_SECONDS = 30 * 60  # 30 minutes
SLIDE_DURATION_SECONDS = 10 * 60   # 10 minutes

# Schema for flight price data
FLIGHT_PRICE_SCHEMA = StructType([
    StructField("airline", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("departure_date", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("seats_available", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("_metadata", StructType([
        StructField("producer_timestamp", DoubleType(), True),
        StructField("route_id", StringType(), True)
    ]), True)
])

# Topics to subscribe to
KAFKA_TOPICS = ["jfk_lax_prices", "ord_mia_prices", "sfo_sea_prices"]