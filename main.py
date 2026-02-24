import os
import psycopg2

def inspect_hours():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("\nGün bazlı min/max saat (TR, 24 hariç):")
    cur.execute("""
        SELECT 
            DATE(ts AT TIME ZONE 'Europe/Istanbul') AS tr_date,
            MIN(ts AT TIME ZONE 'Europe/Istanbul') AS min_time,
            MAX(ts AT TIME ZONE 'Europe/Istanbul') AS max_time
        FROM raw_minute_bars
        WHERE DATE(ts AT TIME ZONE 'Europe/Istanbul') <> '2026-02-24'
        GROUP BY tr_date
        ORDER BY tr_date;
    """)

    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_hours()
