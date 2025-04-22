"""
Batch mode processor for airline price data
"""
import time
import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
import sys
import os

# Add the parent directory to the path so we can import from spark_streaming
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spark_streaming import spark_queries

# PostgreSQL connection properties
DB_PROPERTIES = {
    "user": "airlineuser",
    "password": "airlinepass",
    "driver": "org.postgresql.Driver"
}

DB_PARAMS = {
    "dbname": "airlinepricing",
    "user": "airlineuser",
    "password": "airlinepass",
    "host": "localhost",
    "port": "5432"
}

def create_spark_session():
    """Create a Spark session for batch processing"""
    return (SparkSession.builder
            .appName("AirlinePriceBatchProcessor")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.1")
            .getOrCreate())

def run_batch_processing():
    """Run batch processing on the PostgreSQL data"""
    spark = create_spark_session()
    
    # Read data from PostgreSQL
    jdbc_url = "jdbc:postgresql://localhost:5432/airlinepricing"
    
    # Time the operation
    start_time = time.time()
    
    # Load raw flight price data
    df = (spark.read
          .jdbc(url=jdbc_url, 
                table="raw_flight_prices", 
                properties=DB_PROPERTIES))
    
    # Register the DataFrame as a temporary view
    df.createOrReplaceTempView("flight_prices")
    
    # Run the queries (no window parameter for batch mode)
    route_stats = spark_queries.get_min_max_avg_prices_by_route(spark)
    best_airlines = spark_queries.get_best_airline_by_route(spark)
    
    # Calculate execution time
    execution_time = time.time() - start_time
    
    # Save results to PostgreSQL with execution time
    save_batch_results(route_stats, "batch_route_stats", execution_time)
    save_batch_results(best_airlines, "batch_best_airlines", execution_time)
    
    # Print some statistics
    route_stats.show()
    best_airlines.show()
    
    print(f"Batch processing completed in {execution_time:.2f} seconds")
    
    # Compare with streaming results
    compare_with_streaming_results()
    
    spark.stop()

def save_batch_results(df, table_name, execution_time):
    """Save batch results to PostgreSQL"""
    # Create a new DataFrame with execution_time
    df_with_time = df.withColumn("execution_time", lit(execution_time))
    
    # Write to PostgreSQL
    jdbc_url = "jdbc:postgresql://localhost:5432/airlinepricing"
    df_with_time.write \
        .jdbc(url=jdbc_url, 
              table=table_name, 
              mode="append", 
              properties=DB_PROPERTIES)

def compare_with_streaming_results():
    """Compare batch processing results with streaming results"""
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Compare route stats
        cur.execute("""
            WITH batch_avg AS (
                SELECT AVG(avg_price) AS batch_avg_price, COUNT(*) AS batch_count
                FROM batch_route_stats
                WHERE created_at > NOW() - INTERVAL '1 hour'
            ),
            stream_avg AS (
                SELECT AVG(avg_price) AS stream_avg_price, COUNT(*) AS stream_count
                FROM route_stats
                WHERE created_at > NOW() - INTERVAL '1 hour'
            ),
            batch_time AS (
                SELECT AVG(execution_time) AS batch_time
                FROM batch_route_stats
                WHERE created_at > NOW() - INTERVAL '1 hour'
            ),
            stream_time AS (
                -- Estimate streaming time based on window duration
                SELECT 30*60 AS stream_time -- 30 minutes in seconds
            )
            INSERT INTO comparison_metrics (
                query_type, 
                streaming_execution_time, 
                batch_execution_time, 
                streaming_result_count, 
                batch_result_count, 
                streaming_avg_price, 
                batch_avg_price,
                price_diff_percentage
            )
            SELECT 
                'route_stats' AS query_type,
                s.stream_time AS streaming_execution_time,
                b_time.batch_time AS batch_execution_time,
                s_avg.stream_count AS streaming_result_count,
                b_avg.batch_count AS batch_result_count,
                s_avg.stream_avg_price AS streaming_avg_price,
                b_avg.batch_avg_price AS batch_avg_price,
                CASE 
                    WHEN s_avg.stream_avg_price > 0 THEN 
                        100 * ABS(s_avg.stream_avg_price - b_avg.batch_avg_price) / s_avg.stream_avg_price
                    ELSE 0
                END AS price_diff_percentage
            FROM 
                batch_avg b_avg,
                stream_avg s_avg,
                batch_time b_time,
                stream_time s
        """)
        
        # Compare best airlines
        cur.execute("""
            WITH batch_avg AS (
                SELECT AVG(avg_price) AS batch_avg_price, COUNT(*) AS batch_count
                FROM batch_best_airlines
                WHERE created_at > NOW() - INTERVAL '1 hour'
            ),
            stream_avg AS (
                SELECT AVG(avg_price) AS stream_avg_price, COUNT(*) AS stream_count
                FROM best_airlines
                WHERE created_at > NOW() - INTERVAL '1 hour'
            ),
            batch_time AS (
                SELECT AVG(execution_time) AS batch_time
                FROM batch_best_airlines
                WHERE created_at > NOW() - INTERVAL '1 hour'
            ),
            stream_time AS (
                -- Estimate streaming time based on window duration
                SELECT 30*60 AS stream_time -- 30 minutes in seconds
            )
            INSERT INTO comparison_metrics (
                query_type, 
                streaming_execution_time, 
                batch_execution_time, 
                streaming_result_count, 
                batch_result_count, 
                streaming_avg_price, 
                batch_avg_price,
                price_diff_percentage
            )
            SELECT 
                'best_airlines' AS query_type,
                s.stream_time AS streaming_execution_time,
                b_time.batch_time AS batch_execution_time,
                s_avg.stream_count AS streaming_result_count,
                b_avg.batch_count AS batch_result_count,
                s_avg.stream_avg_price AS streaming_avg_price,
                b_avg.batch_avg_price AS batch_avg_price,
                CASE 
                    WHEN s_avg.stream_avg_price > 0 THEN 
                        100 * ABS(s_avg.stream_avg_price - b_avg.batch_avg_price) / s_avg.stream_avg_price
                    ELSE 0
                END AS price_diff_percentage
            FROM 
                batch_avg b_avg,
                stream_avg s_avg,
                batch_time b_time,
                stream_time s
        """)
        
        conn.commit()
        print("Comparison metrics saved to database")
        
    except Exception as e:
        print(f"Error comparing results: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Starting batch processing...")
    
    # Fix importing lit function
    from pyspark.sql.functions import lit
    
    run_batch_processing()