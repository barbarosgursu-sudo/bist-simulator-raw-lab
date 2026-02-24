import yfinance as yf
import pandas as pd

def check_after_hours():
    print("\nASELS.IS extended test (prepost=True)\n")

    df = yf.download(
        "ASELS.IS",
        period="2d",        # yeterli
        interval="1m",
        auto_adjust=False,
        prepost=True,       # kritik
        progress=False
    )

    if df.empty:
        print("No data returned.")
        return

    df = df.reset_index()

    # TR saatine çevir
    df["TR_Time"] = df["Datetime"].dt.tz_convert("Europe/Istanbul")

    # 17:59 sonrası barlar
    after = df[df["TR_Time"].dt.time > pd.to_datetime("17:59:00").time()]

    print("Toplam bar:", len(df))
    print("En geç saat:", df["TR_Time"].max())

    print("\n17:59 sonrası barlar:")
    print(after[["TR_Time", "Open", "High", "Low", "Close", "Volume"]])

if __name__ == "__main__":
    check_after_hours()
