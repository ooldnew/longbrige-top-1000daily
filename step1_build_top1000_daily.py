import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime
from longbridge.openapi import Config, QuoteContext, Period, AdjustType

# 基础配置
LB_APP_KEY = os.getenv("LP_APP_KEY")
LB_APP_SECRET = os.getenv("LP_APP_SECRET")
LB_ACCESS_TOKEN = os.getenv("LP_ACCESS_TOKEN")

config = Config(LB_APP_KEY, LB_APP_SECRET, LB_ACCESS_TOKEN)
ctx = QuoteContext(config)

YEARS = [2021, 2022, 2023, 2024, 2025]
OUTPUT_CSV = "top1000_daily.csv"

def get_us_tickers():
    df = pd.read_csv("all.csv")
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)].symbol.unique()

def main():
    tickers = get_us_tickers()
    all_daily_records = []

    print(f"正在从长桥获取 {len(tickers)} 只股票的历史成交额数据...")
    
    # 获取所有股票的日线数据（不复权，为了成交额准确）
    for t in tqdm(tickers):
        sym = f"{t}.US"
        try:
            # 1000根K线大约覆盖4年左右，建议根据实际跨度调整数量
            klines = ctx.candlesticks(sym, Period.Day, 1500, AdjustType.NoAdjust)
            for k in klines:
                y = k.timestamp.year
                if y in YEARS:
                    all_daily_records.append({
                        "date": k.timestamp.strftime('%Y-%m-%d'),
                        "symbol": sym,
                        "turnover": float(k.turnover)
                    })
        except Exception:
            continue

    # 转换为 DataFrame 并按日期分组筛选
    df_all = pd.DataFrame(all_daily_records)
    print("\n正在计算每日交易额前1000名...")
    
    # 按日期分组，每组按 turnover 降序排列，取前 1000
    df_top1000 = df_all.groupby("date").apply(
        lambda x: x.sort_values("turnover", ascending=False).head(1000)
    ).reset_index(drop=True)

    df_top1000.to_csv(OUTPUT_CSV, index=False)
    print(f"名单生成成功：{OUTPUT_CSV}")

if __name__ == "__main__":
    main()
