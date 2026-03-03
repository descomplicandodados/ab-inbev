

import psycopg2
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator

load_dotenv()

PROCESS_DATE = datetime.utcnow().strftime("%Y%m%d")

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


class BreweryModel(BaseModel):
    id: str
    name: str | None = None
    brewery_type: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)

    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if v in ("", None):
            return None
        return v


def get_connection():
    logger.debug("Opening database connection...")
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "breweries"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )


def create_table():
    logger.info("Ensuring silver table exists...")
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
        ) PARTITION BY LIST (anomesdia);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_audit (
            layer TEXT,
            process_date TEXT,
            status TEXT,
            failed_records INT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    conn.close()
    logger.info("Silver tables verified.")


def create_partition(conn):
    logger.info(f"Ensuring silver partition exists for {PROCESS_DATE}...")
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS breweries_silver_{PROCESS_DATE}
        PARTITION OF breweries_silver
        FOR VALUES IN ('{PROCESS_DATE}');
    """)

    conn.commit()
    logger.info(f"Partition breweries_silver_{PROCESS_DATE} ready.")


def run():
    logger.info("Starting Silver transformation...")
    start = time.time()

    create_table()

    conn = get_connection()
    create_partition(conn)

    cur = conn.cursor()

    logger.info("Fetching bronze records for processing...")
    cur.execute("""
        SELECT payload FROM breweries_bronze
        WHERE anomesdia = %s
    """, (PROCESS_DATE,))

    rows = cur.fetchall()
    total_rows = len(rows)

    logger.info(f"{total_rows} records fetched from bronze.")

    total_inserted = 0
    failed = 0

    for row in rows:
        record = row[0]

        try:
            validated = BreweryModel(**record)

            cur.execute("""
                INSERT INTO breweries_silver
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT DO NOTHING
            """, (
                validated.id,
                validated.name,
                validated.brewery_type,
                validated.city,
                validated.state,
                validated.country,
                validated.latitude,
                validated.longitude,
                PROCESS_DATE
            ))

            total_inserted += 1

        except ValidationError as e:
            failed += 1
            logger.debug(f"Validation failed for record ID={record.get('id')} | Error={e}")

    error_rate = failed / total_rows if total_rows else 0
    status = "FAILED" if error_rate > 0.05 else "PASSED"

    logger.info(f"Silver processing finished.")
    logger.info(f"Inserted: {total_inserted}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Error rate: {error_rate:.2%}")
    logger.info(f"DQ Status: {status}")

    cur.execute("""
        INSERT INTO data_quality_audit
        VALUES (%s,%s,%s,%s,NOW())
    """, ("silver", PROCESS_DATE, status, failed))

    if status == "FAILED":
        conn.commit()
        logger.error("DQ threshold exceeded. Failing pipeline.")
        raise Exception("DQ FAILED - Silver layer")

    conn.commit()

    duration = time.time() - start

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            layer TEXT,
            process_date TEXT,
            total_records INT,
            duration_seconds FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        INSERT INTO pipeline_metrics
        VALUES (%s,%s,%s,%s,NOW())
    """, ("silver", PROCESS_DATE, total_inserted, duration))

    conn.commit()
    conn.close()

    logger.info(f"Silver completed in {duration:.2f} seconds.")
    logger.info("Database connection closed.")