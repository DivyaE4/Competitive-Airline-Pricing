"""
API wrapper for flight price data (simulated or using Amadeus API)
"""
import random
import time
import json
import datetime
import requests
from config import AMADEUS_API_KEY, AMADEUS_API_SECRET

class FlightDataProvider:
    def __init__(self, use_real_api=False):
        self.use_real_api = use_real_api
        self.token = None
        self.token_expires = 0
        
        # List of airlines for simulated data
        self.airlines = [
            {"code": "DL", "name": "Delta Air Lines"},
            {"code": "UA", "name": "United Airlines"},
            {"code": "AA", "name": "American Airlines"},
            {"code": "WN", "name": "Southwest Airlines"},
            {"code": "B6", "name": "JetBlue Airways"},
            {"code": "AS", "name": "Alaska Airlines"},
            {"code": "F9", "name": "Frontier Airlines"}
        ]
    
    def _get_token(self):
        """Get authentication token for Amadeus API"""
        if not self.use_real_api:
            return "simulated-token"
            
        if self.token and time.time() < self.token_expires:
            return self.token
            
        url = "https://test.api.amadeus.com/v1/security/oauth2/token"
        payload = f"grant_type=client_credentials&client_id={AMADEUS_API_KEY}&client_secret={AMADEUS_API_SECRET}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.token = data["access_token"]
            self.token_expires = time.time() + data["expires_in"] - 300  # Buffer of 5 minutes
            return self.token
        else:
            raise Exception(f"Failed to get API token: {response.text}")
    
    def _get_simulated_prices(self, origin, destination, date_str):
        """Generate simulated flight prices"""
        flights = []
        
        # Generate 3-7 flights per request
        num_flights = random.randint(3, 7)
        
        for _ in range(num_flights):
            # Select random airline
            airline = random.choice(self.airlines)
            
            # Generate random departure and arrival times
            departure_hour = random.randint(6, 20)
            departure_min = random.choice([0, 15, 30, 45])
            flight_duration = random.randint(90, 300)  # 1.5 to 5 hours in minutes
            
            departure_time = f"{departure_hour:02d}:{departure_min:02d}:00"
            
            # Calculate arrival time
            departure_dt = datetime.datetime.strptime(f"{date_str}T{departure_time}", "%Y-%m-%dT%H:%M:%S")
            arrival_dt = departure_dt + datetime.timedelta(minutes=flight_duration)
            arrival_time = arrival_dt.strftime("%H:%M:%S")
            
            # Generate price (more realistic ranges based on routes)
            base_price = 0
            if (origin == 'JFK' and destination == 'LAX') or (origin == 'LAX' and destination == 'JFK'):
                base_price = random.randint(250, 600)
            elif (origin == 'ORD' and destination == 'MIA') or (origin == 'MIA' and destination == 'ORD'):
                base_price = random.randint(180, 450)
            elif (origin == 'SFO' and destination == 'SEA') or (origin == 'SEA' and destination == 'SFO'):
                base_price = random.randint(120, 350)
            else:
                base_price = random.randint(150, 500)
                
            # Add some variance
            price = base_price + random.randint(-50, 100)
            
            flight = {
                "airline": airline["code"],
                "airline_name": airline["name"],
                "flight_number": f"{airline['code']}{random.randint(100, 9999)}",
                "origin": origin,
                "destination": destination,
                "departure_date": date_str,
                "departure_time": departure_time,
                "arrival_time": arrival_time,
                "duration_minutes": flight_duration,
                "price_usd": price,
                "seats_available": random.randint(1, 50),
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            flights.append(flight)
            
        return flights
    
    def get_flight_prices(self, origin, destination):
        """Get flight prices for a specific route"""
        if self.use_real_api:
            # Use real Amadeus API
            token = self._get_token()
            
            tomorrow = datetime.date.today() + datetime.timedelta(days=1)
            date_str = tomorrow.strftime("%Y-%m-%d")
            
            url = f"https://test.api.amadeus.com/v2/shopping/flight-offers"
            headers = {
                "Authorization": f"Bearer {token}"
            }
            params = {
                "originLocationCode": origin,
                "destinationLocationCode": destination,
                "departureDate": date_str,
                "adults": 1,
                "currencyCode": "USD"
            }
            
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                # Parse and transform the response to our format
                # This would need to be adapted based on actual API response format
                data = response.json()
                # Transform actual API response here
                return data
            else:
                print(f"API request failed: {response.text}")
                # Fallback to simulated data
                return self._get_simulated_prices(origin, destination, date_str)
        else:
            # Use simulated data
            tomorrow = datetime.date.today() + datetime.timedelta(days=1)
            date_str = tomorrow.strftime("%Y-%m-%d")
            return self._get_simulated_prices(origin, destination, date_str)