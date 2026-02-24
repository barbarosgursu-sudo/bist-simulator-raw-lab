import os
import psycopg2

def create_daily_official():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_official (
            symbol TEXT NOT NULL,
            session_date DATE NOT NULL,
            official_open NUMERIC,
            official_close NUMERIC,
            source_open TEXT DEFAULT 'YAHOO_09_55_BAR',
            source_close TEXT DEFAULT 'YAHOO_1D',
            created_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (symbol, session_date)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("daily_official table ready")

@app.post("/run-sql")
def run_sql():
    sql = """
    ALTER TABLE daily_official
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

    ALTER TABLE daily_official
      ALTER COLUMN official_open TYPE NUMERIC(18,6),
      ALTER COLUMN official_close TYPE NUMERIC(18,6);

    CREATE INDEX IF NOT EXISTS idx_daily_official_date
      ON daily_official(session_date);
    """

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok"}

if __name__ == "__main__":
    create_daily_official()
