import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException

app = FastAPI(title="BIST Replay Engine - Schema Init")

def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL not found in environment")
    return psycopg2.connect(database_url)

@app.get("/")
def root():
    return {"message": "BIST Replay Engine schema service is running"}

@app.get("/health")
def health():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        one = cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "ok", "db": "connected", "result": one[0]}
    except Exception as e:
        return {"status": "error", "db": "not_connected", "error": str(e)}

@app.post("/init/daily-official")
def init_daily_official():
    ddl = """
    CREATE TABLE IF NOT EXISTS daily_official (
      symbol          VARCHAR(20) NOT NULL,
      session_date    DATE NOT NULL,

      official_open   NUMERIC(18,6) NOT NULL,
      official_close  NUMERIC(18,6) NOT NULL,

      source_open     TEXT NOT NULL,
      source_close    TEXT NOT NULL,

      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

      PRIMARY KEY (symbol, session_date)
    );

    CREATE INDEX IF NOT EXISTS idx_daily_official_date
      ON daily_official (session_date);

    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = now();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS trg_daily_official_updated_at ON daily_official;

    CREATE TRIGGER trg_daily_official_updated_at
    BEFORE UPDATE ON daily_official
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
    """

    try:
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(ddl)
        cur.close()
        conn.close()
        return {"status": "ok", "message": "daily_official table (and trigger/index) ensured"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"init failed: {str(e)}")

@app.get("/schema/daily-official")
def describe_daily_official():
    """Quick check: show columns if table exists."""
    q = """
    SELECT
      column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='daily_official'
    ORDER BY ordinal_position;
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(q)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"table": "daily_official", "columns": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"describe failed: {str(e)}")

def db_info():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_schema(), current_user;")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return {"current_database": row[0], "current_schema": row[1], "current_user": row[2]}
