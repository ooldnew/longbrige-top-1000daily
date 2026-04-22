import pandas as pd
import os
from tqdm import tqdm
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
    if not os.path.exists("all.csv"):
        print("错误：未找到 all.csv 文件")
        return []
    df = pd.read_csv("all.csv")
    return df[df["symbol"].str.match(r"^[A-Z]{1,5}$", na=False)].symbol.unique()

def main():
    tickers = get_us_tickers()
    if len(tickers) == 0:
        return

    all_daily_records = []

    print(f"正在从长桥获取 {len(tickers)} 只股票的历史成交额数据...")
    
    for t in tqdm(tickers):
        sym = f"{t}.US"
        try:
            # 获取 1200 根左右的日线数据
            klines = ctx.candlesticks(sym, Period.Day, 1200, AdjustType.NoAdjust)
            if not klines:
                continue
                
            for k in klines:
                dt = k.timestamp
                if dt.year in YEARS:
                    all_daily_records.append({
                        "date": dt.strftime('%Y-%m-%d'),
                        "symbol": sym,
                        "turnover": float(k.turnover)
                    })
        except Exception as e:
            # 静默跳过错误，避免中断
            continue

    # 检查是否抓取到了数据
    if not all_daily_records:
        print("\n[警告] 未能获取到任何符合日期范围的行情数据，请检查 API 权限或 Token。")
        return

    # 转换为 DataFrame
    df_all = pd.DataFrame(all_daily_records)
    
    print(f"\n成功抓取到 {len(df_all)} 条记录，正在计算每日交易额前1000名...")
    
    # 核心逻辑：按日期分组，取成交额前 1000
    # 使用 nlargest 替代 sort_values.head，效率更高
    df_top1000 = df_all.groupby("date", group_keys=False).apply(
        lambda x: x.nlargest(1000, "turnover")
    )

    # 排序使结果整齐
    df_top1000 = df_top1000.sort_values(["date", "turnover"], ascending=[True, False])

    df_top1000.to_csv(OUTPUT_CSV, index=False)
    print(f"名单生成成功：{OUTPUT_CSV}，总计 {len(df_top1000)} 行。")

if __name__ == "__main__":
    main()
