import os
import psycopg2
import yfinance as yf
import pandas as pd
from datetime import datetime

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
                row["Datetime"],
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row.get("Adj Close", None),
                row["Volume"],
                "YAHOO"
            ))

        conn.commit()

    cur.close()
    conn.close()
    print("INGEST DONE")

if __name__ == "__main__":
    ingest_raw()
