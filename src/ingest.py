import os
import time
import json
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict
from typing import Optional, List
from requests.exceptions import RequestException

# ==================================================
# LOAD ENV
# ==================================================

load_dotenv()

API_URL = os.getenv("API_URL")
PER_PAGE = int(os.getenv("API_PER_PAGE", 200))
TIMEOUT = int(os.getenv("API_TIMEOUT", 30))
SLEEP_SECONDS = float(os.getenv("API_SLEEP_SECONDS", 0.5))

DQ_INVALID_THRESHOLD = float(os.getenv("DQ_INVALID_THRESHOLD", 0.05))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

MAX_RETRIES = 5
BACKOFF_FACTOR = 2
PROCESS_DATE = datetime.utcnow().strftime("%Y%m%d")

# ==================================================
# STRUCTURED LOGGING
# ==================================================

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()

def log_event(level, event_type, message, **kwargs):
    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "event_type": event_type,
        "message": message,
        **kwargs,
    }

    if level == "ERROR":
        logger.error(json.dumps(log_data))
    else:
        logger.info(json.dumps(log_data))


# ==================================================
# MODEL
# ==================================================

class Brewery(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    name: Optional[str] = None
    brewery_type: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


# ==================================================
# DATABASE
# ==================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS breweries_silver (
            id TEXT,
            name TEXT,
            brewery_type TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT,
            anomesdia TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (id, anomesdia)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


# ==================================================
# RETRY
# ==================================================

def request_with_retry(params):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            start = time.time()
            response = requests.get(API_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            latency = round(time.time() - start, 3)

            log_event("INFO", "api_success", "API call success",
                      latency_seconds=latency)

            return response.json()

        except RequestException as e:
            wait = BACKOFF_FACTOR ** attempt
            log_event("ERROR", "api_retry", "API call failed",
                      attempt=attempt, wait_seconds=wait)
            time.sleep(wait)
            attempt += 1

    raise Exception("API failed after max retries")


# ==================================================
# DATA QUALITY
# ==================================================

def validate_batch(batch):
    valid = []
    invalid = 0

    for record in batch:
        try:
            brewery = Brewery(**record)

            if not brewery.id:
                raise ValueError("Missing ID")

            valid.append(brewery)

        except Exception:
            invalid += 1

    total = len(batch)
    invalid_ratio = invalid / total if total > 0 else 0

    log_event(
        "INFO",
        "data_quality",
        "DQ check completed",
        total=total,
        valid=len(valid),
        invalid=invalid,
        invalid_ratio=round(invalid_ratio, 4)
    )

    if invalid_ratio > DQ_INVALID_THRESHOLD:
        log_event(
            "ERROR",
            "dq_failure",
            "Invalid ratio above threshold",
            threshold=DQ_INVALID_THRESHOLD,
            invalid_ratio=invalid_ratio
        )
        raise Exception("Data Quality threshold exceeded")

    return valid


# ==================================================
# LOAD SILVER
# ==================================================

def insert_silver(valid_records):
    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO breweries_silver VALUES (
            %(id)s, %(name)s, %(brewery_type)s,
            %(city)s, %(state)s, %(country)s,
            %(latitude)s, %(longitude)s,
            %(anomesdia)s, NOW()
        )
        ON CONFLICT (id, anomesdia) DO NOTHING;
    """

    records = []

    for brewery in valid_records:
        records.append({
            "id": brewery.id,
            "name": brewery.name,
            "brewery_type": brewery.brewery_type,
            "city": brewery.city,
            "state": brewery.state,
            "country": brewery.country,
            "latitude": brewery.latitude,
            "longitude": brewery.longitude,
            "anomesdia": PROCESS_DATE
        })

    execute_batch(cur, query, records)
    conn.commit()
    cur.close()
    conn.close()


# ==================================================
# PIPELINE
# ==================================================

def run():
    start_time = datetime.utcnow()
    log_event("INFO", "pipeline_start", "Pipeline started",
              process_date=PROCESS_DATE)

    create_tables()

    total_processed = 0
    page = 1

    while True:
        data = request_with_retry({"page": page, "per_page": PER_PAGE})

        if not data:
            break

        valid_records = validate_batch(data)
        insert_silver(valid_records)

        total_processed += len(valid_records)

        log_event("INFO", "batch_processed",
                  "Batch processed",
                  page=page,
                  records=len(valid_records))

        page += 1
        time.sleep(SLEEP_SECONDS)

    if total_processed == 0:
        log_event("ERROR", "pipeline_failure",
                  "No records processed")
        raise Exception("Pipeline processed zero records")

    duration = (datetime.utcnow() - start_time).total_seconds()

    log_event(
        "INFO",
        "pipeline_end",
        "Pipeline completed",
        total_processed=total_processed,
        duration_seconds=duration,
        records_per_second=round(total_processed / duration, 2)
    )


if __name__ == "__main__":
    run()