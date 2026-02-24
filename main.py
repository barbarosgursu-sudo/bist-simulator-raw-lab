import os
import psycopg2
import yfinance as yf
import pandas as pd

SYMBOLS = [
    "ASELS.IS",
    "THYAO.IS",
    "SISE.IS"
]

def ingest_raw():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL not found!")
        return

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    for symbol in SYMBOLS:
        print(f"Fetching {symbol}")

        # 1. Veriyi indir
        df = yf.download(
            symbol,
            period="5d",
            interval="1m",
            auto_adjust=False,
            progress=False
        )

        if df.empty:
            print(f"No data for {symbol}")
            continue

        # 2. MultiIndex yapısını temizle (yfinance bazen sütunları (Price, Ticker) şeklinde verir)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # 3. Index'i sütuna çevir (Datetime sütunu oluşur)
        df = df.reset_index()
        
        # Sütun isimlerini normalize edelim (Hata payını azaltmak için)
        # Genelde 'Datetime' gelir ama garantiye alalım
        date_col = 'Datetime' if 'Datetime' in df.columns else 'Date'

        # 4. itertuples() kullanarak satır satır dön
        for row in df.itertuples(index=False):
            # Adj Close her zaman gelmeyebilir, kontrol edelim
            adj_close = getattr(row, 'Adj_Close', None) # itertuples boşlukları _ yapar
            
            cur.execute("""
                INSERT INTO raw_minute_bars 
                (symbol, ts, open, high, low, close, adj_close, volume, source_timezone)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                symbol,
                getattr(row, date_col), # Dinamik tarih sütunu
                float(row.Open) if pd.notna(row.Open) else None,
                float(row.High) if pd.notna(row.High) else None,
                float(row.Low) if pd.notna(row.Low) else None,
                float(row.Close) if pd.notna(row.Close) else None,
                float(adj_close) if pd.notna(adj_close) else None,
                int(row.Volume) if pd.notna(row.Volume) else None,
                "YAHOO"
            ))

        conn.commit()
        print(f"{symbol} done")

    cur.close()
    conn.close()
    print("INGEST DONE")

if __name__ == "__main__":
    ingest_raw()
