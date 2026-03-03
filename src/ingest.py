import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional, List

load_dotenv()

API_URL = os.getenv("API_URL")
PER_PAGE = int(os.getenv("API_PER_PAGE", 200))
TIMEOUT = int(os.getenv("API_TIMEOUT", 30))
SLEEP_SECONDS = float(os.getenv("API_SLEEP_SECONDS", 0.5))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


class Brewery(BaseModel):
    id: str
    name: Optional[str]
    brewery_type: Optional[str]
    address_1: Optional[str]
    address_2: Optional[str]
    address_3: Optional[str]
    city: Optional[str]
    state: Optional[str]
    state_province: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]
    longitude: Optional[str]
    latitude: Optional[str]
    phone: Optional[str]
    website_url: Optional[str]
    street: Optional[str]

    def to_db_dict(self):
        return {k: (v if v is not None else None) for k, v in self.dict().items()}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table():
    query = """
        CREATE TABLE IF NOT EXISTS breweries (
            id TEXT PRIMARY KEY,
            name TEXT,
            brewery_type TEXT,
            address_1 TEXT,
            address_2 TEXT,
            address_3 TEXT,
            city TEXT,
            state TEXT,
            state_province TEXT,
            postal_code TEXT,
            country TEXT,
            longitude TEXT,
            latitude TEXT,
            phone TEXT,
            website_url TEXT,
            street TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        );
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def insert_batch(records: List[dict]):
    query = """
        INSERT INTO breweries VALUES (
            %(id)s, %(name)s, %(brewery_type)s,
            %(address_1)s, %(address_2)s, %(address_3)s,
            %(city)s, %(state)s, %(state_province)s,
            %(postal_code)s, %(country)s,
            %(longitude)s, %(latitude)s,
            %(phone)s, %(website_url)s, %(street)s, NOW()
        )
        ON CONFLICT (id) DO NOTHING;
    """
    conn = get_connection()
    cur = conn.cursor()
    execute_batch(cur, query, records)
    conn.commit()
    cur.close()
    conn.close()


def fetch_breweries():
    page = 1
    while True:
        response = requests.get(
            API_URL,
            params={"page": page, "per_page": PER_PAGE},
            timeout=TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        yield data
        page += 1
        time.sleep(SLEEP_SECONDS)


def run():
    start = datetime.now()
    print(f"Início: {start}")

    create_table()

    total = 0
    for batch in fetch_breweries():
        validated = [Brewery(**r).to_db_dict() for r in batch]
        insert_batch(validated)
        total += len(validated)
        print(f"{total} registros inseridos")

    print(f"Fim. Duração: {datetime.now() - start}")


if __name__ == "__main__":
    run()