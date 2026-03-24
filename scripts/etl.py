import logging
import requests
import psycopg2
from psycopg2 import errors 
from psycopg2.extras import execute_values
from scripts.config import DB_CONFIG, CITIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"


def extract(city: dict) -> dict | None:
    """Запрашиваем данные о погоде для одного города."""
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current_weather": True,
        "timezone": "Europe/Moscow",
    }
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Ошибка запроса для %s: %s", city["name"], e)
        return None


def transform(data: dict, city_name: str) -> dict | None:
    """Извлекаем нужные поля, возвращаем структурированный dict."""
    try:
        w = data["current_weather"]
        return {
            "city":        city_name,
            "temperature": float(w["temperature"]),
            "windspeed":   float(w["windspeed"]),
            "time":        w["time"],
        }
    except (KeyError, ValueError) as e:
        logger.error("Ошибка трансформации для %s: %s", city_name, e)
        return None


def load(records: list[dict], conn) -> int:
    """Bulk-insert всех записей, пропускаем дубликаты."""
    if not records:
        return 0

    rows = [(r["city"], r["temperature"], r["windspeed"], r["time"]) for r in records]
    inserted = 0
    with conn.cursor() as cur:
        try:
            execute_values(
                cur,
                """
                INSERT INTO weather (city, temperature, windspeed, time)
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
            continue

        record = transform(raw, city["name"])
        if record is None:
            continue

        records.append(record)
        logger.info("%s: %.1f°C, ветер %.1f км/ч", 
                    city["name"], record["temperature"], record["windspeed"])

    if not records:
        logger.warning("Нет данных для записи")
        return

    with psycopg2.connect(**DB_CONFIG) as conn:
        load(records, conn)


if __name__ == "__main__":
    main()