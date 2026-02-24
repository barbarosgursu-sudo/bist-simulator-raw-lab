import os
import psycopg2
import yfinance as yf
import pandas as pd
from fastapi import FastAPI
from datetime import datetime

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

@app.get("/sync-asels-final")
def sync_asels_final_browser():
    """
    ASELS.IS verilerini birleştirir ve sadece tarayıcıya (Browser) yansıtır.
    Açılış: DB'deki 09:55 Close barı
    Kapanış: Yahoo'daki 1D Close barı
    """
    symbol = "ASELS.IS"
    target_dates = ["2026-02-18", "2026-02-19", "2026-02-20", "2026-02-23", "2026-02-24"]
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Kapanışları Yahoo'dan çek (1D barları)
        df_1d = yf.download(symbol, start="2026-02-18", end="2026-02-26", interval="1d", auto_adjust=False, progress=False)
        if isinstance(df_1d.columns, pd.MultiIndex): 
            df_1d.columns = df_1d.columns.get_level_values(0)
        df_1d.index = pd.to_datetime(df_1d.index)

        # 2. Açılışları Veritabanından çek (09:55 barları)
        query_open = """
            SELECT DATE(ts AT TIME ZONE 'Europe/Istanbul') as tr_date, close
            FROM raw_minute_bars
            WHERE symbol = %s 
              AND (ts AT TIME ZONE 'Europe/Istanbul')::time = '09:55:00'
              AND DATE(ts AT TIME ZONE 'Europe/Istanbul') IN %s;
        """
        cur.execute(query_open, (symbol, tuple(target_dates)))
        db_opens = {str(row[0]): float(row[1]) for row in cur.fetchall()}
        
        cur.close()
        conn.close()

        # 3. Verileri Birleştir (Sadece Browser için liste oluştur)
        final_results = []
        for t_date in target_dates:
            # DB'den açılış fiyatını al
            official_open = db_opens.get(t_date, "DB'de Veri Yok")
            
            # Yahoo'dan kapanış fiyatını al
            try:
                official_close = float(df_1d.loc[df_1d.index == pd.to_datetime(t_date), 'Close'].iloc[0])
            except:
                official_close = "Yahoo'da Veri Yok"

            final_results.append({
                "tarih": t_date,
                "resmi_acilis_0955_db": official_open,
                "resmi_kapanis_1d_yahoo": official_close,
                "durum": "OK" if isinstance(official_open, float) and isinstance(official_close, float) else "Eksik Veri"
            })

        return {
            "symbol": symbol,
            "mantik": "Acilis (DB 09:55 Close) + Kapanis (Yahoo 1D Close)",
            "sonuclar": final_results
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
