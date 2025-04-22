"""
Kafka producer for airline pricing data
"""
import time
import json
import threading
from kafka import KafkaProducer
from kafka.errors import KafkaError
from api_wrapper import FlightDataProvider
from config import KAFKA_BOOTSTRAP_SERVERS, ROUTES, REFRESH_INTERVAL

class AirlinePriceProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.data_provider = FlightDataProvider(use_real_api=False)
        
    def publish_prices(self, route):
        """Publish flight prices for a specific route to its Kafka topic"""
        try:
            origin = route['origin']
            destination = route['destination']
            topic = route['topic']
            
            # Get flight prices for this route
            flights = self.data_provider.get_flight_prices(origin, destination)
            
            # Send each flight as a separate message
            for flight in flights:
                # Use airline code as key for partitioning
                key = flight.get('airline', 'unknown')
                
                # Add metadata
                flight['_metadata'] = {
                    'producer_timestamp': time.time(),
                    'route_id': f"{origin}-{destination}"
                }
                
                # Send to Kafka
                self.producer.send(topic, key=key, value=flight)
                
            self.producer.flush()
            print(f"Published {len(flights)} flights for route {origin}-{destination} to topic {topic}")
            
        except KafkaError as e:
            print(f"Error publishing to Kafka: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    def start_publishing(self):
        """Start publishing price data for all routes at regular intervals"""
        def publish_route_periodically(route):
            while True:
                self.publish_prices(route)
                time.sleep(REFRESH_INTERVAL)
        
        # Create a thread for each route
        threads = []
        for route in ROUTES:
            thread = threading.Thread(
                target=publish_route_periodically, 
                args=(route,),
                daemon=True
            )
            threads.append(thread)
            thread.start()
            
        # Wait for all threads
        for thread in threads:
            thread.join()
            
    def create_topics_if_needed(self):
        """Create Kafka topics if they don't exist"""
        # Note: This assumes Kafka is configured to auto-create topics
        # Otherwise, you would need to use the Kafka admin client
        # to create the topics programmatically
        pass

if __name__ == "__main__":
    print("Starting Airline Price Producer...")
    producer = AirlinePriceProducer()
    producer.create_topics_if_needed()
    producer.start_publishing()