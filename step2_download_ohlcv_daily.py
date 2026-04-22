import os
import pandas as pd
from tqdm import tqdm
from longbridge.openapi import Config, QuoteContext, Period, AdjustType

LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
quote_ctx = QuoteContext(config)

BASE_DIR = "us_daily_top1000_ohlcv"
os.makedirs(BASE_DIR, exist_ok=True)

def main():
    # 读取每日 Top1000 名单
    df_list = pd.read_csv("top1000_daily.csv")
    unique_symbols = df_list["symbol"].unique()
    
    print(f"共有 {len(unique_symbols)} 只唯一股票需要下载行情...")

    # 为了效率，按股票下载全量行情后在本地匹配日期
    for sym in tqdm(unique_symbols):
        try:
            # 获取前复权行情
            klines = quote_ctx.candlesticks(sym, Period.Day, 1500, AdjustType.ForwardAdjust)
            rows = [{"date": k.timestamp.strftime("%Y-%m-%d"),
                     "open": float(k.open), "high": float(k.high), 
                     "low": float(k.low), "close": float(k.close), 
                     "volume": int(k.volume), "turnover": float(k.turnover)}
                    for k in klines]
            
            df_stock = pd.DataFrame(rows)
            # 只保留该股票在 Top1000 名单中的那些日期
            target_dates = df_list[df_list["symbol"] == sym]["date"].tolist()
            df_final = df_stock[df_stock["date"].isin(target_dates)]
            
            if not df_final.empty:
                df_final.to_csv(os.path.join(BASE_DIR, f"{sym.replace('.','_')}.csv"), index=False)
        except Exception:
            continue

if __name__ == "__main__":
    main()
