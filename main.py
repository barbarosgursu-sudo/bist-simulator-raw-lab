import yfinance as yf

def get_daily_close():
    df = yf.download(
        "ASELS.IS",
        period="7d",
        interval="1d",
        auto_adjust=False,
        progress=False
    )

    print("\nASELS.IS 1D Close değerleri:\n")

    for date, row in df.iterrows():
        print(date.date(), float(row["Close"]))

if __name__ == "__main__":
    get_daily_close()
