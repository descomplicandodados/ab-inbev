

import requests
import psycopg2
import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROCESS_DATE = datetime.utcnow().strftime("%Y%m%d")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "breweries"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )

def create_table():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS breweries_bronze (
            id TEXT,
            payload JSONB,
            anomesdia TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        ) PARTITION BY LIST (anomesdia);
    """)

    conn.commit()
    cur.close()
    conn.close()

def create_partition(conn):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS breweries_bronze_{PROCESS_DATE}
        PARTITION OF breweries_bronze
        FOR VALUES IN ('{PROCESS_DATE}');
    """)
    conn.commit()

def log_metrics(conn, total, duration):
    cur = conn.cursor()
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
    """, ("bronze", PROCESS_DATE, total, duration))

    conn.commit()

def run():
    start = time.time()
    create_table()

    conn = get_connection()
    create_partition(conn)

    page = 1
    total_inserted = 0

    while True:
        response = requests.get(
            "https://api.openbrewerydb.org/v1/breweries",
            params={"page": page, "per_page": 200}
        )

        data = response.json()
        if not data:
            break

        cur = conn.cursor()

        for record in data:
            cur.execute("""
                INSERT INTO breweries_bronze (id, payload, anomesdia)
                VALUES (%s,%s,%s)
            """, (
                record.get("id"),
                json.dumps(record),
                PROCESS_DATE
            ))
            total_inserted += 1

        conn.commit()
        page += 1

    duration = time.time() - start
    log_metrics(conn, total_inserted, duration)
    conn.close()