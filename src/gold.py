

import psycopg2
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROCESS_DATE = datetime.utcnow().strftime("%Y%m%d")

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


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
    logger.info("Ensuring gold table exists...")
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS breweries_gold (
            state TEXT,
            total_breweries INT,
            anomesdia TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        ) PARTITION BY LIST (anomesdia);
    """)

    conn.commit()
    conn.close()
    logger.info("Gold table verified.")


def create_partition(conn):
    logger.info(f"Ensuring gold partition for {PROCESS_DATE}...")
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS breweries_gold_{PROCESS_DATE}
        PARTITION OF breweries_gold
        FOR VALUES IN ('{PROCESS_DATE}');
    """)

    conn.commit()
    logger.info(f"Partition breweries_gold_{PROCESS_DATE} ready.")


def run():
    logger.info("Starting Gold aggregation...")
    start = time.time()

    create_table()

    conn = get_connection()
    create_partition(conn)
    cur = conn.cursor()

    logger.info(f"Cleaning previous gold data for {PROCESS_DATE}...")
    cur.execute("""
        DELETE FROM breweries_gold
        WHERE anomesdia = %s
    """, (PROCESS_DATE,))
    conn.commit()

    logger.info("Aggregating silver data into gold...")

    cur.execute("""
        INSERT INTO breweries_gold (state, total_breweries, anomesdia)
        SELECT
            state,
            COUNT(*) as total_breweries,
            anomesdia
        FROM breweries_silver
        WHERE anomesdia = %s
        GROUP BY state, anomesdia
    """, (PROCESS_DATE,))

    inserted_rows = cur.rowcount
    conn.commit()

    logger.info(f"{inserted_rows} aggregated rows inserted into gold.")

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
    """, ("gold", PROCESS_DATE, inserted_rows, duration))

    conn.commit()
    conn.close()

    logger.info(f"Gold completed in {duration:.2f} seconds.")
    logger.info("Database connection closed.")