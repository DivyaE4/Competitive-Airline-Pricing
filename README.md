# Competitive Airline Pricing Reports

This project implements a real-time and batch processing system for analyzing airline pricing data across three fixed routes using Apache Kafka, Apache Spark Streaming, and PostgreSQL.

## Project Structure

- **kafka_producer/**: Kafka producer components to fetch/simulate flight price data
- **spark_streaming/**: Spark Structured Streaming components for real-time processing
- **db_batch_processing/**: Batch processing components and database utilities
- **shared/**: Shared resources and sample data

## Prerequisites

- Python 3.8+
- Apache Kafka 
- Apache Spark 3.x with PySpark
- PostgreSQL
- Python packages listed in `requirements.txt`

## Setup Instructions

1. Clone the repository
   ```
   git clone https://github.com/your-username/competitive-airline-pricing.git
   cd competitive-airline-pricing
   ```

2. Install required Python packages
   ```
   pip install -r requirements.txt
   ```

3. Initialize PostgreSQL database
   ```
   psql -U airlineuser -d airlinepricing -f db_batch_processing/create_schema.sql
   ```

## Running the Application

### 1. Start Kafka (if not already running)
```bash
# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

# In a new terminal, start Kafka broker
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

### 2. Create Kafka topics
```bash
$KAFKA_HOME/bin/kafka-topics.sh --create --topic jfk_lax_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic ord_mia_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic sfo_sea_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Start the Kafka Producer
```bash
python kafka_producer/producer.py
```

### 4. Start the Spark Streaming Processor
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.1 spark_streaming/streaming_processor.py
```

### 5. Run Batch Processing (periodically)
```bash
spark-submit --packages org.postgresql:postgresql:42.5.1 db_batch_processing/batch_mode_processor.py
```

### 6. Generate Visualizations and Comparison Metrics
```bash
python db_batch_processing/visualizations.py
python db_batch_processing/comparison_metrics.py
```

## Project Components

### Kafka Producer
Simulates flight price data for three fixed routes:
- JFK-LAX (New York to Los Angeles)
- ORD-MIA (Chicago to Miami)
- SFO-SEA (San Francisco to Seattle)

### Spark Streaming
Processes real-time flight price data with the following features:
- Window-based aggregations (30-minute tumbling windows)
- Minimum, maximum, and average prices by route
- Best airline identification (lowest average price)
- Price trend analysis

### PostgreSQL Storage
Stores both raw and processed data:
- Raw flight prices from Kafka
- Streaming results (route statistics, best airlines)
- Batch processing results
- Comparison metrics

### Batch Processing
Runs the same queries as streaming but on historical data:
- Route statistics (min, max, avg)
- Best airlines by route
- Performance comparison with streaming

### Visualization
Generates plots for:
- Price trends by route
- Best airlines by route 
- Performance comparison between streaming and batch

## Evaluation

The system evaluates:
- Execution time differences between streaming and batch
- Result accuracy (price differences between modes)
- Resource utilization

## Authors

- Member 1: [Your Name] - Kafka Producer
- Member 2: [Your Name] - Spark Streaming
- Member 3: [Your Name] - Batch Processing & Visualization