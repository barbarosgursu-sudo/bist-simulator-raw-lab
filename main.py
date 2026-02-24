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

@app.get("/test-asels-yahoo")
def test_asels_yahoo():
    import yfinance as yf
    import pandas as pd
    from datetime import datetime

    symbol = "ASELS.IS"
    
    try:
        # 1. Resmi Kapanış (1d) verisini çek
        df_1d = yf.download(symbol, period="1d", interval="1d", auto_adjust=False, progress=False)
        
        # 2. Resmi Açılış (09:55 1m) verisini çek
        # Not: Bugünün verisini almak için 7 günlük çekip içinden bugünü süzmek en sağlıklısıdır
        df_1m = yf.download(symbol, period="1d", interval="1m", auto_adjust=False, progress=False)

        # MultiIndex temizliği
        if isinstance(df_1d.columns, pd.MultiIndex): df_1d.columns = df_1d.columns.get_level_values(0)
        if isinstance(df_1m.columns, pd.MultiIndex): df_1m.columns = df_1m.columns.get_level_values(0)

        # 1D Kapanış Değeri
        official_close = float(df_1d['Close'].iloc[-1]) if not df_1d.empty else "Bulunamadı"

        # 09:55 1m Açılış Değeri (Bugün içindeki 09:55 barını bul)
        official_open = "Bulunamadı"
        if not df_1m.empty:
            # Index'i datetime'a çevir ve 09:55'i süz
            df_1m.index = pd.to_datetime(df_1m.index)
            opening_bar = df_1m.between_time('09:55', '09:55')
            if not opening_bar.empty:
                official_open = float(opening_bar['Open'].iloc[0])

        return {
            "kaynak": "Yahoo Finance",
            "symbol": symbol,
            "tarih": datetime.now().strftime("%Y-%m-%d"),
            "resmi_acilis_0955_1m_open": official_open,
            "resmi_kapanis_1d_close": official_close,
            "not": "Eğer değerler 'Bulunamadı' ise seans henüz o saate ulaşmamış veya Yahoo veriyi geciktiriyor olabilir."
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    # Railway PORT değişkenini otomatik atar, yerelde 8000 varsayılan olur
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
    
