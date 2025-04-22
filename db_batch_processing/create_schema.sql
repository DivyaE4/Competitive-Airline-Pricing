-- Schema for airline pricing data
-- This file initializes the PostgreSQL database schema

-- Raw flight price data
CREATE TABLE IF NOT EXISTS raw_flight_prices (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(10),
    airline_name VARCHAR(100),
    flight_number VARCHAR(10),
    origin VARCHAR(3),
    destination VARCHAR(3),
    departure_date DATE,
    departure_time TIME,
    arrival_time TIME,
    duration_minutes INTEGER,
    price_usd NUMERIC(10, 2),
    seats_available INTEGER,
    timestamp TIMESTAMP,
    producer_timestamp NUMERIC(20, 6),
    route_id VARCHAR(7),
    topic VARCHAR(50),
    kafka_timestamp TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_flight_prices_route ON raw_flight_prices(origin, destination);
CREATE INDEX IF NOT EXISTS idx_raw_flight_prices_timestamp ON raw_flight_prices(timestamp);

-- Route statistics from streaming
CREATE TABLE IF NOT EXISTS route_stats (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(3),
    destination VARCHAR(3),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    min_price NUMERIC(10, 2),
    max_price NUMERIC(10, 2),
    avg_price NUMERIC(10, 2),
    flight_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_route_stats_window ON route_stats(window_start, window_end);

-- Best airlines by route from streaming
CREATE TABLE IF NOT EXISTS best_airlines (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(3),
    destination VARCHAR(3),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    airline VARCHAR(10),
    airline_name VARCHAR(100),
    avg_price NUMERIC(10, 2),
    flight_count INTEGER,
    price_rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Price trends by route from streaming
CREATE TABLE IF NOT EXISTS price_trends (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(3),
    destination VARCHAR(3),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_price NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store batch processing results
CREATE TABLE IF NOT EXISTS batch_route_stats (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(3),
    destination VARCHAR(3),
    min_price NUMERIC(10, 2),
    max_price NUMERIC(10, 2),
    avg_price NUMERIC(10, 2),
    flight_count INTEGER,
    execution_time NUMERIC(10, 3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store batch processing best airlines
CREATE TABLE IF NOT EXISTS batch_best_airlines (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(3),
    destination VARCHAR(3),
    airline VARCHAR(10),
    airline_name VARCHAR(100),
    avg_price NUMERIC(10, 2),
    flight_count INTEGER,
    price_rank INTEGER,
    execution_time NUMERIC(10, 3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store comparison metrics
CREATE TABLE IF NOT EXISTS comparison_metrics (
    id SERIAL PRIMARY KEY,
    query_type VARCHAR(50),
    streaming_execution_time NUMERIC(10, 3),
    batch_execution_time NUMERIC(10, 3),
    streaming_result_count INTEGER,
    batch_result_count INTEGER,
    streaming_avg_price NUMERIC(10, 2),
    batch_avg_price NUMERIC(10, 2),
    price_diff_percentage NUMERIC(5, 2),
    comparison_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);