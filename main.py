import os
import psycopg2

def inspect_asels_closing_bar():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("\nASELS günlük son bar (24 hariç, TR):\n")

    cur.execute("""
        SELECT DISTINCT ON (DATE(ts AT TIME ZONE 'Europe/Istanbul'))
            DATE(ts AT TIME ZONE 'Europe/Istanbul') AS tr_date,
            (ts AT TIME ZONE 'Europe/Istanbul') AS tr_time,
            close,
            volume
        FROM raw_minute_bars
        WHERE symbol = 'ASELS.IS'
          AND DATE(ts AT TIME ZONE 'Europe/Istanbul') <> '2026-02-24'
        ORDER BY 
            DATE(ts AT TIME ZONE 'Europe/Istanbul'),
            ts DESC;
    """)

    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_asels_closing_bar()
