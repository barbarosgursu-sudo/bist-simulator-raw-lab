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
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    for symbol in SYMBOLS:
        print(f"Fetching {symbol}")

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

        df = df.reset_index()

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO raw_minute_bars
                (symbol, ts, open, high, low, close, adj_close, volume, source_timezone)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                symbol,
                row["Datetime"].to_pydatetime() if pd.notna(row["Datetime"]) else None,
                float(row["Open"]) if pd.notna(row["Open"]) else None,
                float(row["High"]) if pd.notna(row["High"]) else None,
                float(row["Low"]) if pd.notna(row["Low"]) else None,
                float(row["Close"]) if pd.notna(row["Close"]) else None,
                float(row["Adj Close"]) if "Adj Close" in df.columns and pd.notna(row.get("Adj Close")) else None,
                int(row["Volume"]) if pd.notna(row["Volume"]) else None,
                "YAHOO"
            ))

        conn.commit()
        print(f"{symbol} done")

    cur.close()
    conn.close()
    print("INGEST DONE")

if __name__ == "__main__":
    ingest_raw()
