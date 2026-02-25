import os
import psycopg2
import psycopg2.extras
import yfinance as yf
import pandas as pd
import pytz
from fastapi import FastAPI
from datetime import datetime, timedelta

app = FastAPI()

TR_TZ = pytz.timezone("Europe/Istanbul")

PILOT_SYMBOLS = ["THYAO.IS", "ASELS.IS", "AKBNK.IS", "BRKSN.IS", "VANGD.IS"]


def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


# -------------------------------------------------------
# UTIL: Son 5 TAM işlem günü hesapla (bugün hariç)
# -------------------------------------------------------

def get_last_5_complete_trading_days():
    today_tr = datetime.now(TR_TZ).date()
    d = today_tr - timedelta(days=1)

    trading_days = []

    while len(trading_days) < 5:
        if d.weekday() < 5:  # 0-4 = Mon-Fri
            trading_days.append(d)
        d -= timedelta(days=1)

    trading_days.sort()
    return trading_days


# -------------------------------------------------------
# RESET
# -------------------------------------------------------

@app.get("/reset-all-data")
def reset_all_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE raw_minute_bars;")
        cur.execute("TRUNCATE TABLE daily_official;")
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# -------------------------------------------------------
# PILOT INGEST (OTOMATİK 5 TAM GÜN)
# -------------------------------------------------------

@app.get("/pilot-ingest-v2")
def pilot_ingest_v2(ca_threshold: float = 0.02):

    if os.getenv("DATASET_LOCKED", "0") == "1":
        return {"status": "blocked", "message": "dataset locked"}

    try:
        trading_days = get_last_5_complete_trading_days()

        start = trading_days[0]
        end = trading_days[-1] + timedelta(days=1)

        conn = get_db_connection()
        conn.autocommit = False

        summary = []

        for symbol in PILOT_SYMBOLS:

            df_1m = yf.download(
                symbol,
                start=str(start),
                end=str(end),
                interval="1m",
                auto_adjust=False,
                progress=False
            )

            if isinstance(df_1m.columns, pd.MultiIndex):
                df_1m.columns = df_1m.columns.get_level_values(0)

            if df_1m.empty:
                summary.append({"symbol": symbol, "status": "no_data"})
                continue

            if df_1m.index.tz is None:
                df_1m.index = df_1m.index.tz_localize("UTC")

            df_1m = df_1m.tz_convert(TR_TZ)

            # Seans filtresi
            df_1m = df_1m.between_time("10:00", "17:59")

            df_1m["minute_index"] = ((df_1m.index.hour - 10) * 60 + df_1m.index.minute) + 1
            df_1m["session_date"] = df_1m.index.date

            df_1m = df_1m[(df_1m["minute_index"] >= 1) & (df_1m["minute_index"] <= 480)]

            # CA detection
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

            # daily_official
            df_1d = yf.download(
                symbol,
                start=str(start),
                end=str(end),
                interval="1d",
                auto_adjust=False,
                progress=False
            )

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

        return {
            "status": "success",
            "trading_days": trading_days,
            "summary": summary
        }

    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return {"status": "error", "message": str(e)}


# -------------------------------------------------------
# HEALTH REPORT
# -------------------------------------------------------

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

@app.get("/missing-minutes")
def missing_minutes(symbol: str, session_date: str):
    """
    Eksik minute_index listesini döndürür.
    Örn:
    /missing-minutes?symbol=THYAO.IS&session_date=2026-02-19
    """
    try:
        # session_date parse
        d = datetime.strptime(session_date, "%Y-%m-%d").date()

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT minute_index
            FROM raw_minute_bars
            WHERE symbol = %s AND session_date = %s
            ORDER BY minute_index;
        """, (symbol, d))

        existing = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        existing_set = set(existing)
        full_set = set(range(1, 481))
        missing = sorted(list(full_set - existing_set))

        # Eksik minute_index'leri TR saate çevir
        # minute_index 1 = 10:00, 480 = 17:59
        missing_tr_times = []
        for mi in missing:
            total_minutes = mi - 1
            hour = 10 + (total_minutes // 60)
            minute = total_minutes % 60
            missing_tr_times.append(f"{hour:02d}:{minute:02d}")

        return {
            "status": "ok",
            "symbol": symbol,
            "session_date": session_date,
            "existing_count": len(existing),
            "missing_count": len(missing),
            "missing_minute_index": missing,
            "missing_tr_times": missing_tr_times
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
