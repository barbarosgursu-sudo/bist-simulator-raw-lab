import os
import psycopg2

def create_raw_table():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_minute_bars (
            symbol TEXT,
            ts TIMESTAMPTZ,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            adj_close NUMERIC,
            volume BIGINT,
            source_timezone TEXT,
            fetch_time TIMESTAMPTZ DEFAULT now()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("raw_minute_bars table ready")

if __name__ == "__main__":
    create_raw_table()
