import os
import psycopg2
import psycopg2.extras
import yfinance as yf
import pandas as pd
import pytz
from fastapi import FastAPI
from datetime import datetime

app = FastAPI()

TR_TZ = pytz.timezone("Europe/Istanbul")

PILOT_SYMBOLS = ["THYAO.IS", "ASELS.IS", "AKBNK.IS", "BRKSN.IS", "VANGD.IS"]


def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    return psycopg2.connect(db_url)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "bse-pilot-ingest"}


@app.get("/db-test")
def db_test():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "connected", "db_response": result[0]}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# -------------------------------------------------------
# RAW TABLE INIT
# -------------------------------------------------------

@app.get("/init-raw-minute-bars-v2")
def init_raw_minute_bars_v2():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS raw_minute_bars;")

        cur.execute("""
            CREATE TABLE raw_minute_bars (
                symbol TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                session_date DATE NOT NULL,
                minute_index SMALLINT NOT NULL CHECK (minute_index BETWEEN 1 AND 480),
                open NUMERIC(12,4) NOT NULL,
                high NUMERIC(12,4) NOT NULL,
                low NUMERIC(12,4) NOT NULL,
                close NUMERIC(12,4) NOT NULL,
                adj_close NUMERIC(12,4) NOT NULL,
                volume BIGINT NOT NULL,
                PRIMARY KEY (symbol, session_date, minute_index)
            );
        """)

        cur.execute("CREATE INDEX idx_rmb_session_date ON raw_minute_bars(session_date);")
        cur.execute("CREATE INDEX idx_rmb_symbol_session_date ON raw_minute_bars(symbol, session_date);")

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/reset-daily-official")
def reset_daily_official():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE daily_official;")
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# -------------------------------------------------------
# PILOT INGEST
# -------------------------------------------------------

@app.get("/pilot-ingest-v2")
def pilot_ingest_v2(start: str, end: str, ca_threshold: float):
    if os.getenv("DATASET_LOCKED", "0") == "1":
        return {"status": "blocked", "message": "dataset locked"}

    try:
        conn = get_db_connection()
        conn.autocommit = False

        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()

        summary = []

        for symbol in PILOT_SYMBOLS:

            df_1m = yf.download(symbol, start=start, end=end, interval="1m", auto_adjust=False, progress=False)
            if isinstance(df_1m.columns, pd.MultiIndex):
                df_1m.columns = df_1m.columns.get_level_values(0)

            if df_1m.empty:
                summary.append({"symbol": symbol, "status": "no_data"})
                continue

            if df_1m.index.tz is None:
                df_1m.index = df_1m.index.tz_localize("UTC")

            df_1m = df_1m.tz_convert(TR_TZ)

            df_1m = df_1m.between_time("10:00", "17:59")

            df_1m["minute_index"] = ((df_1m.index.hour - 10) * 60 + df_1m.index.minute) + 1
            df_1m["session_date"] = df_1m.index.date

            df_1m = df_1m[(df_1m["minute_index"] >= 1) & (df_1m["minute_index"] <= 480)]

            if "Adj Close" in df_1m.columns:
                ratio = ((df_1m["Close"] - df_1m["Adj Close"]).abs() / df_1m["Close"]).max()
                if ratio > ca_threshold:
                    summary.append({"symbol": symbol, "status": "excluded_ca", "ratio": float(ratio)})
                    continue

            rows = []
            df_utc = df_1m.tz_convert("UTC")

            for i in range(len(df_utc)):
                rows.append((
                    symbol,
                    df_utc.index[i].to_pydatetime(),
                    df_1m["session_date"].iloc[i],
                    int(df_1m["minute_index"].iloc[i]),
                    float(df_1m["Open"].iloc[i]),
                    float(df_1m["High"].iloc[i]),
                    float(df_1m["Low"].iloc[i]),
                    float(df_1m["Close"].iloc[i]),
                    float(df_1m["Adj Close"].iloc[i]),
                    int(df_1m["Volume"].iloc[i]) if pd.notna(df_1m["Volume"].iloc[i]) else 0
                ))

            insert_sql = """
                INSERT INTO raw_minute_bars
                (symbol, ts, session_date, minute_index, open, high, low, close, adj_close, volume)
                VALUES %s
                ON CONFLICT (symbol, session_date, minute_index) DO NOTHING;
            """

            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=5000)

            # Daily official
            df_1d = yf.download(symbol, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
            if isinstance(df_1d.columns, pd.MultiIndex):
                df_1d.columns = df_1d.columns.get_level_values(0)

            daily_rows = []
            for idx in df_1d.index:
                d = pd.to_datetime(idx).date()
                daily_rows.append((
                    symbol,
                    d,
                    float(df_1d.loc[idx, "Open"]),
                    float(df_1d.loc[idx, "Close"]),
                    "yahoo",
                    "yahoo",
                    datetime.utcnow(),
                    datetime.utcnow()
                ))

            daily_sql = """
                INSERT INTO daily_official
                (symbol, session_date, official_open, official_close, source_open, source_close, created_at, updated_at)
                VALUES %s
                ON CONFLICT (symbol, session_date)
                DO UPDATE SET
                    official_open = EXCLUDED.official_open,
                    official_close = EXCLUDED.official_close,
                    updated_at = EXCLUDED.updated_at;
            """

            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, daily_sql, daily_rows, page_size=1000)

            summary.append({"symbol": symbol, "status": "ok", "bars": len(rows)})

        conn.commit()
        conn.close()

        return {"status": "success", "summary": summary}

    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return {"status": "error", "message": str(e)}


@app.get("/data-health-report")
def data_health_report():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT symbol, session_date, COUNT(*), MIN(minute_index), MAX(minute_index)
            FROM raw_minute_bars
            GROUP BY symbol, session_date
            ORDER BY session_date DESC, symbol;
        """)
        stats = cur.fetchall()

        cur.close()
        conn.close()

        return {"status": "ok", "raw_stats": stats}

    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
