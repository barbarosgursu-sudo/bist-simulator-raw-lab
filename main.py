import yfinance as yf
import pandas as pd

# ASELS.IS sembolünü tanımlayalım
symbol = "ASELS.IS"

# Belirttiğin tarihler (Başlangıç dahil, bitiş hariç tutulur)
# 18, 19 ve 20'yi alabilmek için bitişi 21 Şubat yapıyoruz
start_date = "2026-02-18"
end_date = "2026-02-21"

def get_daily_closes():
    print(f"{symbol} için günlük kapanışlar getiriliyor...")
    
    # 1d (günlük) periyot ile veriyi indir
    df = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        print("Veri bulunamadı.")
        return

    # MultiIndex yapısını temizle
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Sadece tarih ve kapanış fiyatlarını gösterelim
    df = df.reset_index()
    
    # İhtiyacımız olan sütunları seçelim
    result = df[['Date', 'Close', 'Adj Close']]
    
    print("\n--- Kapanış Fiyatları ---")
    print(result.to_string(index=False))

if __name__ == "__main__":
    get_daily_closes()
