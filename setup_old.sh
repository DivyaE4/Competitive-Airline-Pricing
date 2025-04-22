#!/bin/bash

# Setup script for the Competitive Airline Pricing project
# This script sets up the environment and initializes the database

# Make script exit if any command fails
set -e

echo "Starting setup for Competitive Airline Pricing project..."

# Check if Python is installed
if ! [ -x "$(command -v python3)" ]; then
  echo 'Error: Python 3 is not installed.' >&2
  exit 1
fi

# Check if pip is installed
if ! [ -x "$(command -v pip)" ]; then
  echo 'Error: pip is not installed.' >&2
  exit 1
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Check if PostgreSQL is installed
if ! [ -x "$(command -v psql)" ]; then
  echo 'Warning: PostgreSQL is not installed or not in PATH.' >&2
  echo 'You will need to set up the database manually.' >&2
else
  # Create PostgreSQL database and user if they don't exist
  echo "Setting up PostgreSQL database..."
  
  # Check if database exists
  if ! psql -lqt | cut -d \| -f 1 | grep -qw airlinepricing; then
    echo "Creating database 'airlinepricing'..."
    createdb airlinepricing
  else
    echo "Database 'airlinepricing' already exists."
  fi
  
  # Check if user exists
  if ! psql -t -c '\du' | cut -d \| -f 1 | grep -qw airlineuser; then
    echo "Creating user 'airlineuser'..."
    psql -c "CREATE USER airlineuser WITH PASSWORD 'airlinepass'"
    psql -c "GRANT ALL PRIVILEGES ON DATABASE airlinepricing TO airlineuser"
  else
    echo "User 'airlineuser' already exists."
  fi
  
  # Initialize database schema
  echo "Initializing database schema..."
  psql -U airlineuser -d airlinepricing -f db_batch_processing/create_schema.sql
fi

# Check if Kafka is installed
if ! [ -x "$(command -v kafka-topics.sh)" ] && ! [ -d "$KAFKA_HOME" ]; then
  echo 'Warning: Kafka is not installed or KAFKA_HOME is not set.' >&2
  echo 'You will need to create Kafka topics manually.' >&2
else
  # Create Kafka topics if they don't exist
  echo "Setting up Kafka topics..."
  
  # Check if topics exist
  KAFKA_TOPICS=("jfk_lax_prices" "ord_mia_prices" "sfo_sea_prices")
  for topic in "${KAFKA_TOPICS[@]}"; do
    if ! kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -q "^$topic$"; then
      echo "Creating Kafka topic '$topic'..."
      kafka-topics.sh --create --topic "$topic" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    else
      echo "Kafka topic '$topic' already exists."
    fi
  done
fi

# Create directories for output files
mkdir -p figures

echo "Setup completed successfully!"
echo "To start the application:"
echo "1. Make sure Kafka and Zookeeper are running"
echo "2. Start the Kafka producer:      python kafka_producer/producer.py"
echo "3. Start the Spark processor:     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.5.1 spark_streaming/streaming_processor.py"
# echo "3. Start the Spark processor:     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.1 spark_streaming/streaming_processor.py"
echo "4. Run batch processing:          spark-submit --packages org.postgresql:postgresql:42.5.1 db_batch_processing/batch_mode_processor.py"
echo "5. Generate visualizations:       python db_batch_processing/visualizations.py"