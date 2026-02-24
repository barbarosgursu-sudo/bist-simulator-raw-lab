import os
import psycopg2
from fastapi import FastAPI

app = FastAPI()

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    return psycopg2.connect(db_url)

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "bist-simulator-raw-lab"}

@app.get("/db-test")
def db_test():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "connected", "db_response": result[0]}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/init-table")
def create_daily_official():
    conn = get_db_connection()
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

    return {"status": "table_ready"}

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

@app.get("/check-daily")
def check_daily():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # tablo var mı?
        cur.execute("SELECT to_regclass('public.daily_official');")
        table_exists = cur.fetchone()[0]

        if table_exists is None:
            cur.close()
            conn.close()
            return {"table_exists": False}

        # kolonlar
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'daily_official'
            ORDER BY ordinal_position;
        """)
        columns = cur.fetchall()

        # indexler
        cur.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'daily_official';
        """)
        indexes = cur.fetchall()

        cur.close()
        conn.close()

        return {
            "table_exists": True,
            "columns": columns,
            "indexes": indexes
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/test-asels")
def test_asels():
    """ASELS.IS için bugün gerçekleşen 09:55 (1m) ve resmi (1d) kapanışı getirir."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. Sabah 09:55 1m barının Close değerini çek
        # Not: Senin isteğine göre Close değerini çekiyoruz.
        cur.execute("""
            SELECT close FROM raw_minute_bars 
            WHERE symbol = 'ASELS.IS' 
              AND CAST(timestamp AS DATE) = CURRENT_DATE
              AND CAST(timestamp AS TIME) = '09:55:00'
            LIMIT 1;
        """)
        open_bar_res = cur.fetchone()
        open_val = open_bar_res[0] if open_bar_res else "Veri bulunamadı"

        # 2. Bugünün resmi 1d barının Close değerini çek
        # Interval kolonun farklıysa burayı 'period' veya 'timeframe' yapabilirsin.
        cur.execute("""
            SELECT close FROM raw_minute_bars 
            WHERE symbol = 'ASELS.IS' 
              AND CAST(timestamp AS DATE) = CURRENT_DATE
              AND interval = '1d'
            LIMIT 1;
        """)
        close_bar_res = cur.fetchone()
        close_val = close_bar_res[0] if close_bar_res else "Veri bulunamadı"

        cur.close()
        conn.close()

        return {
            "symbol": "ASELS.IS",
            "tarih": str(psycopg2.sql.Literal(psycopg2.extensions.AsIs('CURRENT_DATE'))), # Bugün
            "resmi_acilis_0955_1m_close": open_val,
            "resmi_kapanis_1d_close": close_val,
            "bilgi": "Eğer 'Veri bulunamadı' yazıyorsa, henüz o bar veritabanına girmemiş olabilir."
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    # Railway PORT değişkenini otomatik atar, yerelde 8000 varsayılan olur
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
    
