from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, min, max, count, expr, rank
from pyspark.sql.window import Window

def register_temp_view(df, view_name):
    """Register a temporary view for a DataFrame"""
    df.createOrReplaceTempView(view_name)

def get_min_max_avg_prices_by_route(spark, window_duration=None):
    """
    Get minimum, maximum, and average prices by route
    If window_duration is provided, compute over the specified window
    """
    if window_duration:
        # Streaming mode with window
        query = f"""
        SELECT 
            origin,
            destination,
            window.start as window_start,
            window.end as window_end,
            MIN(price_usd) as min_price,
            MAX(price_usd) as max_price,
            AVG(price_usd) as avg_price,
            COUNT(*) as flight_count
        FROM flight_prices
        GROUP BY origin, destination, window(timestamp, '{window_duration}')
        """
    else:
        # Batch mode, no window
        query = """
        SELECT 
            origin,
            destination,
            MIN(price_usd) as min_price,
            MAX(price_usd) as max_price,
            AVG(price_usd) as avg_price,
            COUNT(*) as flight_count
        FROM flight_prices
        GROUP BY origin, destination
        """
    
    try:
        return spark.sql(query)
    except Exception as e:
        print(f"Error in get_min_max_avg_prices_by_route: {str(e)}")
        # Return empty DataFrame with same schema as a fallback
        return spark.createDataFrame([], 
            "origin STRING, destination STRING, window_start TIMESTAMP, window_end TIMESTAMP, min_price DOUBLE, max_price DOUBLE, avg_price DOUBLE, flight_count LONG")

def get_best_airline_by_route(spark, window_duration=None):
    """
    Find the airline with the lowest average price by route
    If window_duration is provided, compute over the specified window
    """
    if window_duration:
        # Streaming mode with window
        query = f"""
        WITH airline_avg_prices AS (
            SELECT
                origin,
                destination,
                airline,
                airline_name,
                window.start as window_start,
                window.end as window_end,
                AVG(price_usd) as avg_price,
                COUNT(*) as flight_count
            FROM flight_prices
            GROUP BY origin, destination, airline, airline_name, window(timestamp, '{window_duration}')
        )
        SELECT
            origin,
            destination,
            window_start,
            window_end,
            airline,
            airline_name,
            avg_price,
            flight_count
        FROM airline_avg_prices
        WHERE (origin, destination, window_start, window_end, avg_price) IN (
            SELECT origin, destination, window_start, window_end, MIN(avg_price)
            FROM airline_avg_prices
            GROUP BY origin, destination, window_start, window_end
        )
        """
    else:
        # Batch mode, no window
        query = """
        WITH airline_avg_prices AS (
            SELECT
                origin,
                destination,
                airline,
                airline_name,
                AVG(price_usd) as avg_price,
                COUNT(*) as flight_count
            FROM flight_prices
            GROUP BY origin, destination, airline, airline_name
        )
        SELECT
            origin,
            destination,
            airline,
            airline_name,
            avg_price,
            flight_count
        FROM airline_avg_prices
        WHERE (origin, destination, avg_price) IN (
            SELECT origin, destination, MIN(avg_price)
            FROM airline_avg_prices
            GROUP BY origin, destination
        )
        """
    
    try:
        return spark.sql(query)
    except Exception as e:
        print(f"Error in get_best_airline_by_route: {str(e)}")
        # Return empty DataFrame with same schema as a fallback
        return spark.createDataFrame([], 
            "origin STRING, destination STRING, window_start TIMESTAMP, window_end TIMESTAMP, airline STRING, airline_name STRING, avg_price DOUBLE, flight_count LONG")

def get_price_trends_by_route(spark, window_duration=None):
    """
    Get price trends over time by route
    If window_duration is provided, compute over the specified window
    """
    if window_duration:
        # Streaming mode with window - REMOVED ORDER BY clause
        query = f"""
        SELECT
            origin,
            destination,
            window.start as window_start,
            window.end as window_end,
            AVG(price_usd) as avg_price
        FROM flight_prices
        GROUP BY origin, destination, window(timestamp, '{window_duration}')
        """
    else:
        # Batch mode - group by date instead of window
        query = """
        SELECT
            origin,
            destination,
            DATE(timestamp) as date,
            AVG(price_usd) as avg_price
        FROM flight_prices
        GROUP BY origin, destination, DATE(timestamp)
        ORDER BY origin, destination, date
        """
    
    try:
        return spark.sql(query)
    except Exception as e:
        print(f"Error in get_price_trends_by_route: {str(e)}")
        # Return empty DataFrame with same schema as a fallback
        return spark.createDataFrame([], 
            "origin STRING, destination STRING, window_start TIMESTAMP, window_end TIMESTAMP, avg_price DOUBLE")