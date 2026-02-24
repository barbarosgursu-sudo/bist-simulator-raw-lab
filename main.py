import os
import psycopg2

def inspect_955_closes():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("\n09:55 bar CLOSE değerleri (24 hariç, TR):\n")

    cur.execute("""
        SELECT 
            symbol,
            DATE(ts AT TIME ZONE 'Europe/Istanbul') AS tr_date,
            close
        FROM raw_minute_bars
        WHERE 
            DATE(ts AT TIME ZONE 'Europe/Istanbul') <> '2026-02-24'
            AND (ts AT TIME ZONE 'Europe/Istanbul')::time = '09:55:00'
        ORDER BY tr_date, symbol;
    """)

    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_955_closes()
