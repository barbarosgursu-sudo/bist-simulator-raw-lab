import os
import psycopg2

def test_db():
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print(cur.fetchone())
    cur.close()
    conn.close()

if __name__ == "__main__":
    test_db()
