# Competitive Airline Pricing Analysis

This project implements a real-time and batch processing system for analyzing airline pricing data across three fixed routes using **Apache Kafka**, **Apache Spark Streaming**, and **PostgreSQL**. It provides insights into flight prices, trends, and airline performance through real-time streaming and batch processing.

---

## Features

- **Real-Time Data Processing**: Stream flight price data using Kafka and process it with Spark Streaming.
- **Batch Processing**: Analyze historical flight price data stored in PostgreSQL.
- **Data Visualization**: Generate visualizations for price trends, best airlines, and performance comparisons.
- **Performance Comparison**: Evaluate execution time and result accuracy between streaming and batch modes.
- **Database Integration**: Store raw and processed data in PostgreSQL for further analysis.

---

## Project Structure

- **`kafka_producer/`**: Simulates or fetches flight price data and publishes it to Kafka topics.
- **`spark_streaming/`**: Processes real-time flight price data using Spark Structured Streaming.
- **`db_batch_processing/`**: Handles batch processing, database schema initialization, and visualizations.
- **`requirements.txt`**: Lists Python dependencies.
- **`.env`**: Stores API keys and environment variables.

---

## Prerequisites

- **Python**: Version 3.8+
- **Apache Kafka**: For real-time data streaming.
- **Apache Spark**: Version 3.x with PySpark.
- **PostgreSQL**: For data storage and analysis.
- **Python Libraries**: Install dependencies from `requirements.txt`.

---

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/competitive-airline-pricing.git
cd competitive-airline-pricing
