import os
import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime

# Veritabanı Bağlantı Bilgileri
# Sunucuda (Railway vb.) DATABASE_URL veya ayrı değişkenler olarak tanımlanmalıdır.
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

def setup_database(cursor):
    """Tablo, Index ve Trigger kurulumlarını yapar."""
    # 1. Tablo Oluşturma
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_official (
            symbol VARCHAR(20) NOT NULL,
            session_date DATE NOT NULL,
            official_open NUMERIC(18,6) NOT NULL,
            official_close NUMERIC(18,6) NOT NULL,
            source_open TEXT NOT NULL,
            source_close TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (symbol, session_date)
        );
    """)

    # 2. Index Oluşturma
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_official_session_date ON daily_official (session_date);")

    # 3. Trigger ve Fonksiyon (updated_at için)
    cursor.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """)
    
    cursor.execute("DROP TRIGGER IF EXISTS set_updated_at ON daily_official;")
    cursor.execute("""
        CREATE TRIGGER set_updated_at
        BEFORE UPDATE ON daily_official
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """)

def save_to_db(df, symbol):
    """Veriyi veritabanına kaydeder."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Tablo şemasını hazırla
        setup_database(cursor)

        # Verileri INSERT et (Conflict durumunda güncelle - Upsert)
        for date, row in df.iterrows():
            cursor.execute("""
                INSERT INTO daily_official 
                (symbol, session_date, official_open, official_close, source_open, source_close)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, session_date) 
                DO UPDATE SET 
                    official_open = EXCLUDED.official_open,
                    official_close = EXCLUDED.official_close,
                    source_open = EXCLUDED.source_open,
                    source_close = EXCLUDED.source_close;
            """, (
                symbol, date.date(), 
                float(row["Open"]), float(row["Close"]), 
                "yfinance", "yfinance"
            ))
        
        print(f"{symbol} verileri başarıyla DB'ye kaydedildi.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Veritabanı Hatası: {e}")

def get_and_save_data():
    symbol = "ASELS.IS"
    start_date = "2026-02-18"
    end_date = "2026-02-21"

    print(f"{symbol} verileri çekiliyor...")
    df = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        print("Veri bulunamadı.")
        return

    # MultiIndex temizliği
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Veritabanına kaydet
    save_to_db(df, symbol)

if __name__ == "__main__":
    get_and_save_data()
