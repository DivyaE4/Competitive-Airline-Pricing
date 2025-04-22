#!/bin/bash

set -e

echo "🚀 Minimal setup: initializing DB and Kafka topics..."

# === PostgreSQL DB schema init ===
echo "📐 Initializing PostgreSQL schema..."
psql -U airlineuser -d airlinepricing -f db_batch_processing/create_schema.sql

# === Kafka Topics ===
echo "📡 Checking/Creating Kafka topics..."
TOPICS=("jfk_lax_prices" "ord_mia_prices" "sfo_sea_prices")

KAFKA_TOPICS_SCRIPT="/home/divya-eshwar/kafka_2.12-3.7.1/bin/kafka-topics.sh"  # Path to kafka-topics.sh

for topic in "${TOPICS[@]}"; do
  if ! $KAFKA_TOPICS_SCRIPT --list --bootstrap-server localhost:9092 | grep -q "^$topic$"; then
    echo "📡 Creating topic: $topic"
    $KAFKA_TOPICS_SCRIPT --create --topic "$topic" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  else
    echo "📡 Topic '$topic' already exists."
  fi
done

# === Output folders ===
mkdir -p figures

echo "✅ Setup done! You're ready to roll."

