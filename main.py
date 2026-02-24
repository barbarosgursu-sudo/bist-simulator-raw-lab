import os
import psycopg2

def inspect_days():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("\nGün bazlı satır sayısı:")
    cur.execute("""
        SELECT DATE(ts AT TIME ZONE 'Europe/Istanbul') AS tr_date,
               COUNT(*)
        FROM raw_minute_bars
        GROUP BY tr_date
        ORDER BY tr_date;
    """)
    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_days()
