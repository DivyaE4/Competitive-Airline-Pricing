"""
Configuration settings for the Kafka producer component
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Topic names for the three routes
TOPICS = {
    'JFK_LAX': 'jfk_lax_prices',
    'ORD_MIA': 'ord_mia_prices',
    'SFO_SEA': 'sfo_sea_prices'
}

# API configuration (if using real API)
AMADEUS_API_KEY = os.getenv('AMADEUS_API_KEY', '')
AMADEUS_API_SECRET = os.getenv('AMADEUS_API_SECRET', '')

# Route definitions
ROUTES = [
    {'origin': 'JFK', 'destination': 'LAX', 'topic': TOPICS['JFK_LAX']},
    {'origin': 'ORD', 'destination': 'MIA', 'topic': TOPICS['ORD_MIA']},
    {'origin': 'SFO', 'destination': 'SEA', 'topic': TOPICS['SFO_SEA']}
]

# Data refresh interval (in seconds)
REFRESH_INTERVAL = 30