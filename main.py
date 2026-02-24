import os
import psycopg2

def inspect_hour_blocks():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("\nSaat bloklarına göre bar sayısı (24 hariç, TR):\n")

    query = """
    SELECT 
        DATE(ts AT TIME ZONE 'Europe/Istanbul') AS tr_date,
        CASE
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time BETWEEN '09:55:00' AND '09:59:59'
                THEN '09:55-10:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '10:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '11:00:00'
                THEN '10:00-11:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '11:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '12:00:00'
                THEN '11:00-12:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '12:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '13:00:00'
                THEN '12:00-13:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '13:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '14:00:00'
                THEN '13:00-14:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '14:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '15:00:00'
                THEN '14:00-15:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '15:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '16:00:00'
                THEN '15:00-16:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '16:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time < '17:00:00'
                THEN '16:00-17:00'
            WHEN (ts AT TIME ZONE 'Europe/Istanbul')::time >= '17:00:00'
                 AND (ts AT TIME ZONE 'Europe/Istanbul')::time <= '17:59:59'
                THEN '17:00-17:59'
        END AS hour_block,
        COUNT(*)
    FROM raw_minute_bars
    WHERE DATE(ts AT TIME ZONE 'Europe/Istanbul') <> '2026-02-24'
    GROUP BY tr_date, hour_block
    ORDER BY tr_date, hour_block;
    """

    cur.execute(query)

    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_hour_blocks()
