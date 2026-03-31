import logging
import requests
import psycopg2
import json
from psycopg2.extras import execute_values
from datetime import datetime
from pathlib import Path
from scripts.config import DB_CONFIG, CITIES
from scripts.spark_job import run_spark

def save_raw(data, city_name):
    date = datetime.utcnow().strftime("%Y-%m-%d")
    path = Path(f"data/raw/{date}")
    path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%H-%M-%S")

    with open(path / f"{city_name}_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(data, f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"


def extract(city: dict) -> dict | None:
    """Запрашиваем данные о погоде для 9 городов."""
    params = {
    "latitude": city["lat"],
    "longitude": city["lon"],
    "hourly": "temperature_2m,wind_speed_10m,precipitation",
    "timezone": "UTC",
    }
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Ошибка запроса для %s: %s", city["name"], e)
        return None


def transform(data: dict, city_name: str) -> list[dict]:
    try:
        result = []

        times = data["hourly"]["time"]
        temps = data["hourly"]["temperature_2m"]
        wind = data["hourly"]["wind_speed_10m"]
        precipitation = data["hourly"]["precipitation"]

        for t, temp, w, p in zip(times, temps, wind, precipitation):
            result.append({
                "city": city_name,
                "time": t,
                "temperature": float(temp),
                "windspeed": float(w),
                "precipitation": float(p),
            })

        return result

    except Exception as e:
        logger.error("Ошибка трансформации для %s: %s", city_name, e)
        return []


def load(records: list[dict], conn) -> int:
    """Bulk-insert всех записей, пропускаем дубликаты."""
    if not records:
        return 0

    rows = [(r["city"], r["temperature"], r["windspeed"], r["precipitation"], r["time"]) 
            for r in records
    ]
    
    logger.info("Подготовлено %d строк для вставки", len(rows))

    inserted = 0
    with conn.cursor() as cur:
        try:
            execute_values(
                cur,
                """
                INSERT INTO weather (city, temperature, windspeed, precipitation, time)
                VALUES %s
                ON CONFLICT (city, time) DO NOTHING
                """,
                rows,
            )
            inserted = cur.rowcount
            conn.commit()
            logger.info("Записано %d строк в БД", inserted)
        except Exception as e:
            conn.rollback()
            logger.error("Ошибка записи в БД: %s", e)
    return inserted


def main():
    records = []

    for city in CITIES:
        raw = extract(city)
        if raw is None:
            logger.warning("Нет данных для %s", city["name"])
            continue

        save_raw(raw, city["name"])

        records_city = transform(raw, city["name"])
        records.extend(records_city)

        logger.info("%s: получено %d записей", city["name"], len(records_city))

    if not records:
        logger.warning("Нет данных для записи")
        return
    
    logger.info("Передаем %d записей в Spark", len(records))

    df = run_spark(records)

    with psycopg2.connect(**DB_CONFIG) as conn:
        load(records, conn)


if __name__ == "__main__":
    main()