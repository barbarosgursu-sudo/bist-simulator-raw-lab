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

@app.get("/fetch-asels-history")
def fetch_asels_history():
    import yfinance as yf
    import pandas as pd
    from datetime import datetime

    symbol = "ASELS.IS"
    # 18'inden 24'üne kadar olan günleri kapsayacak aralık
    start_date = "2026-02-18"
    end_date = "2026-02-25" # 24'ünü dahil etmek için bir gün sonrası
    
    results = []
    
    try:
        # 1. Günlük (1d) barları çek
        df_1d = yf.download(symbol, start=start_date, end=end_date, interval="1d", auto_adjust=False, progress=False)
        
        # 2. Dakikalık (1m) barları çek (09:55'leri yakalamak için)
        df_1m = yf.download(symbol, start=start_date, end=end_date, interval="1m", auto_adjust=False, progress=False)

        # MultiIndex temizliği
        if isinstance(df_1d.columns, pd.MultiIndex): df_1d.columns = df_1d.columns.get_level_values(0)
        if isinstance(df_1m.columns, pd.MultiIndex): df_1m.columns = df_1m.columns.get_level_values(0)

        # Tarih indexlerini düzeltme
        df_1d.index = pd.to_datetime(df_1d.index)
        df_1m.index = pd.to_datetime(df_1m.index)

        # Belirttiğin tarihler üzerinde dönelim
        target_dates = ["2026-02-18", "2026-02-19", "2026-02-20", "2026-02-23", "2026-02-24"]

        for target_date in target_dates:
            date_obj = pd.to_datetime(target_date)
            
            # Kapanış Değeri (1d tablosundan o güne bak)
            try:
                close_val = float(df_1d.loc[df_1d.index == date_obj, 'Close'].iloc[0])
            except:
                close_val = "Bulunamadı (Resmi tatil veya veri eksik)"

            # Açılış Değeri (1m tablosundan 09:55'e bak)
            try:
                # O güne ait 09:55 barını süz
                day_1m = df_1m[df_1m.index.date == date_obj.date()]
                opening_bar = day_1m.between_time('09:55', '09:55')
                open_val = float(opening_bar['Open'].iloc[0])
            except:
                open_val = "Bulunamadı"

            results.append({
                "tarih": target_date,
                "resmi_acilis_0955_1m_open": open_val,
                "resmi_kapanis_1d_close": close_val
            })

        return {
            "symbol": symbol,
            "data": results,
            "kaynak": "Yahoo Finance"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/fetch-asels-correct")
def fetch_asels_correct():
    import yfinance as yf
    import pandas as pd

    symbol = "ASELS.IS"
    start_date = "2026-02-18"
    end_date = "2026-02-25"
    
    results = []
    
    try:
        # 1. 1d verisini çek (Kapanış için)
        df_1d = yf.download(symbol, start=start_date, end=end_date, interval="1d", auto_adjust=False, progress=False)
        
        # 2. 1m verisini çek (09:55 barı için)
        df_1m = yf.download(symbol, start=start_date, end=end_date, interval="1m", auto_adjust=False, progress=False)

        if isinstance(df_1d.columns, pd.MultiIndex): df_1d.columns = df_1d.columns.get_level_values(0)
        if isinstance(df_1m.columns, pd.MultiIndex): df_1m.columns = df_1m.columns.get_level_values(0)

        df_1d.index = pd.to_datetime(df_1d.index)
        df_1m.index = pd.to_datetime(df_1m.index)

        target_dates = ["2026-02-18", "2026-02-19", "2026-02-20", "2026-02-23", "2026-02-24"]

        for target_date in target_dates:
            date_obj = pd.to_datetime(target_date)
            
            # Kapanış: 1d barının Close değeri
            try:
                final_close = float(df_1d.loc[df_1d.index == date_obj, 'Close'].iloc[0])
            except:
                final_close = None

            # Açılış: 09:55 1m barının CLOSE değeri (Senin paylaştığın koddaki doğru mantık)
            try:
                day_1m = df_1m[df_1m.index.date == date_obj.date()]
                opening_bar = day_1m.between_time('09:55', '09:55')
                # DİKKAT: Burası artık .open değil, .close
                official_open = float(opening_bar['Close'].iloc[0])
            except:
                official_open = None

            if final_close and official_open:
                results.append({
                    "tarih": target_date,
                    "resmi_acilis_0955_1m_close": official_open,
                    "resmi_kapanis_1d_close": final_close
                })

        return {
            "symbol": symbol,
            "data": results,
            "mantik": "Acilis=09:55_1m_Close, Kapanis=1d_Close"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

Bu fonksiyonu bir FastAPI endpoint'ine dönüştürerek main.py dosyana ekledim. Fonksiyonun içindeki ts ve Europe/Istanbul kullanımı, veritabanındaki zaman damgası (timestamp) yapısına sadık kalarak hazırlandı.

Bunu tarayıcıdan (Browser) kolayca görebilmen için @app.get olarak tanımladım.

main.py Dosyasına Eklenecek Bölüm
Python

@app.get("/inspect-955")
def inspect_955_closes_endpoint():
    """
    Veritabanındaki raw_minute_bars tablosundan 
    Türkiye saatine göre 09:55 barlarının CLOSE değerlerini listeler.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Senin sağladığın SQL sorgusu
        # Not: 24 Şubat hariç tutulmuştur
        query = """
            SELECT 
                symbol,
                DATE(ts AT TIME ZONE 'Europe/Istanbul') AS tr_date,
                close
            FROM raw_minute_bars
            WHERE 
                DATE(ts AT TIME ZONE 'Europe/Istanbul') <> '2026-02-24'
                AND (ts AT TIME ZONE 'Europe/Istanbul')::time = '09:55:00'
            ORDER BY tr_date, symbol;
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        # Sonuçları JSON formatına çevirelim
        results = []
        for row in rows:
            results.append({
                "symbol": row[0],
                "date": str(row[1]),
                "close_at_0955": float(row[2])
            })

        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "description": "09:55 bar CLOSE değerleri (TR saatiyle)",
            "data": results
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    # Railway PORT değişkenini otomatik atar, yerelde 8000 varsayılan olur
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
    
