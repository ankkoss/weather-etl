# Weather ETL Pipeline

ETL-пайплайн который собирает данные о погоде через open-meteo API 
и сохраняет их в PostgreSQL.

## Стек
- Python 3.10
- PostgreSQL 17
- psycopg2, requests, python-dotenv

## Архитектура
Extract → Transform → Load

- **Extract** — запрос к open-meteo API для каждого города
- **Transform** — валидация и типизация данных
- **Load** — bulk-insert в PostgreSQL, дубликаты пропускаются

## Структура проекта
weather-etl/
├── scripts/
│   ├── etl.py      # основной пайплайн
│   └── config.py   # конфигурация
├── sql/
│   └── schema.sql
├── .env            # секреты (не в git)
├── .gitignore
├── requirements.txt
└── README.md

## Установка и запуск

1. Клонировать репозиторий
git clone https://github.com/ankkoss/weather-etl

2. Установить зависимости
pip install -r requirements.txt

3. Создать .env файл
DB_PASSWORD=ваш_пароль
DB_NAME=weather_db
DB_USER=postgres
DB_HOST=localhost
DB_PORT=5432

4. Создать таблицу в PostgreSQL
psql -U postgres -d weather_db -f schema.sql

5. Запустить
python etl.py

## Пример вывода
2024-01-15 10:23:01 [INFO] Moscow: -5.2°C, ветер 12.3 км/ч
2024-01-15 10:23:02 [INFO] Riga: 1.1°C, ветер 8.7 км/ч
2024-01-15 10:23:02 [INFO] Записано 2 строк в БД

## Города
Москва, Рига — легко расширяется через config.py