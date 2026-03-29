import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "weather_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

CITIES = [
    {"name": "Moscow", "lat": 55.75, "lon": 37.61},
    {"name": "Riga",   "lat": 56.95, "lon": 24.10},
    {"name": "Tokio", "lat": 35.68, "lon": 139.69},
    {"name": "Shanghai", "lat":31.23,"lon": 121.46},
    {"name": "London", "lat": 51.50, "lon": -0.125},
    {"name": "Cairo", "lat": 30.05, "lon": 31.24},
    {"name": "New York", "lat": 40.71, "lon": -74.006},
    {"name": "Sydney", "lat": -33.86, "lon": 151.20},
    {"name": "Seoul", "lat": 37.56, "lon": 126.97},
    
]