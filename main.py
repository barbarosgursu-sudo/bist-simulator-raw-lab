import os
import psycopg2

def inspect():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("\nToplam satır:")
    cur.execute("SELECT COUNT(*) FROM raw_minute_bars;")
    print(cur.fetchone())

    print("\nSembol bazlı satır sayısı:")
    cur.execute("""
        SELECT symbol, COUNT(*) 
        FROM raw_minute_bars 
        GROUP BY symbol;
    """)
    for row in cur.fetchall():
        print(row)

    print("\nZaman aralığı:")
    cur.execute("""
        SELECT MIN(ts), MAX(ts) 
        FROM raw_minute_bars;
    """)
    print(cur.fetchone())

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect()
